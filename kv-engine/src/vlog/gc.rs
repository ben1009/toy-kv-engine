use std::sync::Arc;

use ahash::AHashSet;

use anyhow::{Ok, Result, anyhow};
use bytes::Bytes;

use crate::{
    lsm_storage::{CasEntry, LsmStorageInner, VersionedCasEntry},
    vlog::{KvKind, ValueLog, ValuePointer, builder::ValueLogWriter},
};

/// Lightweight reference to a live vLog entry (no value payload).
#[derive(Debug)]
pub struct LiveEntryRef {
    pub ptr: ValuePointer,
    pub key: Vec<u8>,
}

/// Result of analyzing a vLog file for GC.
#[derive(Debug)]
pub struct GcAnalysis {
    pub file_id: u32,
    pub stale_ratio: f64,
    pub live_entries: Vec<LiveEntryRef>,
    pub dead_bytes: usize,
    pub total_bytes: usize,
}

/// Result of a GC compaction operation.
#[derive(Clone, Copy, Debug)]
pub struct GcResult {
    pub old_file_id: u32,
    pub new_file_id: u32,
    pub keys_rewritten: usize,
    pub bytes_written: u64,
}

/// Garbage collector for vLog files.
///
/// Scans vLog files to find live vs. dead entries, and rewrites live entries
/// to new vLog files when the stale ratio exceeds the configured threshold.
pub struct GarbageCollector<'a> {
    vlog: &'a Arc<ValueLog>,
    inner: &'a LsmStorageInner,
    threshold: f64,
}

impl<'a> GarbageCollector<'a> {
    pub(crate) fn new(vlog: &'a Arc<ValueLog>, inner: &'a LsmStorageInner, threshold: f64) -> Self {
        Self {
            vlog,
            inner,
            threshold,
        }
    }

    /// Analyze a vLog file to determine live vs. dead entries.
    /// Uses the vLog index when available (O(1) key lookup, no header reads);
    /// falls back to header-only iteration.
    pub fn analyze_file(&self, file_id: u32) -> Result<GcAnalysis> {
        // Try to use the index for faster liveness checks.
        if let std::result::Result::Ok(index) = self.vlog.get_or_rebuild_index(file_id) {
            return self.analyze_file_with_index(file_id, &index);
        }

        // Fallback: header-only iteration
        let reader = self.vlog.get_reader(file_id)?;
        let header_iter = reader.iter_headers()?;

        let mut live_entries = Vec::new();
        let mut dead_bytes = 0usize;
        let mut total_bytes = 0usize;

        for meta_result in header_iter {
            let meta = meta_result?;
            total_bytes += meta.entry_size;

            let is_live = self.check_liveness(&meta.key, &meta.ptr)?;
            if is_live {
                live_entries.push(LiveEntryRef {
                    ptr: meta.ptr,
                    key: meta.key,
                });
            } else {
                dead_bytes += meta.entry_size;
            }
        }

        let stale_ratio = if total_bytes > 0 {
            dead_bytes as f64 / total_bytes as f64
        } else {
            0.0
        };

        Ok(GcAnalysis {
            file_id,
            stale_ratio,
            live_entries,
            dead_bytes,
            total_bytes,
        })
    }

    /// Analyze using the vLog index — avoids reading vLog headers entirely.
    fn analyze_file_with_index(
        &self,
        file_id: u32,
        index: &crate::vlog::VlogIndex,
    ) -> Result<GcAnalysis> {
        let mut live_entries = Vec::new();
        let mut dead_bytes = 0usize;
        let mut total_bytes = 0usize;

        for entry in index.entries() {
            // Compute the entry size from the index metadata.
            // Propagate error on overflow (indicates corrupt index data).
            let entry_size = crate::vlog::VlogEntryHeader::compute_entry_size(
                entry.key.len(),
                entry.value_len as usize,
            )
            .ok_or_else(|| {
                anyhow!(
                    "entry size overflow for key len={}, value len={}",
                    entry.key.len(),
                    entry.value_len
                )
            })?;
            total_bytes += entry_size;

            let size: u32 = entry_size.try_into().map_err(|_| {
                anyhow!(
                    "entry_size {} exceeds u32::MAX for key len={}, value len={}",
                    entry_size,
                    entry.key.len(),
                    entry.value_len
                )
            })?;

            let ptr = crate::vlog::ValuePointer {
                file_id,
                offset: entry.offset,
                size,
            };

            let is_live = self.check_liveness(&entry.key, &ptr)?;
            if is_live {
                live_entries.push(LiveEntryRef {
                    ptr,
                    key: entry.key.clone(),
                });
            } else {
                dead_bytes += entry_size;
            }
        }

        let stale_ratio = if total_bytes > 0 {
            dead_bytes as f64 / total_bytes as f64
        } else {
            0.0
        };

        Ok(GcAnalysis {
            file_id,
            stale_ratio,
            live_entries,
            dead_bytes,
            total_bytes,
        })
    }

    /// Check if a vLog entry is still live (the LSM tree still points to it).
    ///
    /// With MVCC, `key` is the full encoded internal key (user key + ts).
    /// Uses version-specific lookup (`get_with_kind_at_ts`) so GC correctly
    /// identifies older versions as live when newer versions exist.
    pub fn check_liveness(&self, key: &[u8], ptr: &ValuePointer) -> Result<bool> {
        let (current_val, current_kind) = if crate::key::TS_ENABLED {
            // Extract user key and ts from the full internal key for
            // version-specific lookup.
            match (
                crate::key::decode_user_key(key),
                crate::key::extract_ts(key),
            ) {
                (Some(user_key), Some(ts)) => self.inner.get_with_kind_at_ts(&user_key, ts)?,
                // Malformed or legacy keys (no ts / no user key) — treat as dead.
                _ => return Ok(false),
            }
        } else {
            self.inner.get_with_kind(key)?
        };

        match current_kind {
            KvKind::ValuePointer => {
                if let Some(ref val) = current_val
                    && let Some(current_ptr) = ValuePointer::try_decode(&val[1..])
                {
                    return Ok(current_ptr.file_id == ptr.file_id
                        && current_ptr.offset == ptr.offset
                        && current_ptr.size == ptr.size);
                }
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    /// Compact a vLog file: rewrite live entries to a new file and CAS each key.
    pub fn compact_file(&self, analysis: &GcAnalysis) -> Result<Option<GcResult>> {
        if analysis.stale_ratio < self.threshold {
            return Ok(None);
        }
        if analysis.live_entries.is_empty() {
            // All entries are dead — just schedule deletion
            self.vlog.schedule_deletion(analysis.file_id);
            self.vlog.record_gc_result(0, 0);
            return Ok(Some(GcResult {
                old_file_id: analysis.file_id,
                new_file_id: u32::MAX, // sentinel: no new file was created
                keys_rewritten: 0,
                bytes_written: 0,
            }));
        }

        // Phase 1: Read live entries and write to a new vLog file
        let new_file_id = self.vlog.next_file_id();
        let new_path = self.vlog.path_of_file(new_file_id);

        // Wrap compaction in a closure so we can clean up the new file on error
        let compact_res = (|| -> Result<GcResult> {
            let mut writer = ValueLogWriter::create(new_path.clone(), new_file_id)?;

            let cache_enabled = self.vlog.value_cache.is_some();
            let mut rewrites: Vec<(Vec<u8>, Option<Bytes>, ValuePointer, ValuePointer)> =
                Vec::with_capacity(analysis.live_entries.len());

            for live_ref in &analysis.live_entries {
                let (key, value) = self.vlog.read_entry(&live_ref.ptr)?;
                let bytes_written = writer.append(&key, &value)?;
                let new_ptr = ValuePointer {
                    file_id: new_file_id,
                    offset: writer.offset() - bytes_written as u64,
                    size: bytes_written as u32,
                };
                let cached_value = if cache_enabled {
                    Some(Bytes::from(value))
                } else {
                    None
                };
                rewrites.push((key, cached_value, live_ref.ptr, new_ptr));
            }

            // Take entries before closing the writer (for index building)
            let index_entries = writer.take_entries();

            // Fsync the new vLog before binding pointers into the LSM tree
            let total_bytes = writer.offset();
            writer.close()?;

            // Persist the vLog index for the new file
            if let Err(e) = self.vlog.save_index(new_file_id, index_entries) {
                eprintln!(
                    "warning: failed to save vLog index for {}: {}",
                    new_file_id, e
                );
            }
            // Sync the directory to ensure the new file's directory entry is durable
            if let std::result::Result::Ok(dir) = std::fs::File::open(&self.vlog.path) {
                let _ = dir.sync_all();
            }

            // Phase 2: Batch CAS all keys under a single lock acquisition.
            // With MVCC, use version-aware CAS so we update the exact version
            // rather than the newest version of each key.
            if crate::key::TS_ENABLED {
                let mut batch: Vec<VersionedCasEntry> = Vec::with_capacity(rewrites.len());
                for (key, _value, old_ptr, new_ptr) in &rewrites {
                    // Keys from live entries are guaranteed well-formed by check_liveness.
                    let user_key = crate::key::decode_user_key(key)
                        .expect("key from live vLog entry must be a valid internal key");
                    let ts = crate::key::extract_ts(key)
                        .expect("key from live vLog entry must have a timestamp");

                    let mut old_buf = Vec::with_capacity(1 + ValuePointer::encoded_size());
                    old_buf.push(KvKind::ValuePointer as u8);
                    old_ptr.encode(&mut old_buf);

                    let mut new_buf = Vec::with_capacity(ValuePointer::encoded_size());
                    new_ptr.encode(&mut new_buf);

                    batch.push((
                        user_key,
                        ts,
                        old_buf,
                        KvKind::ValuePointer,
                        new_buf,
                        KvKind::ValuePointer,
                    ));
                }
                let cas_results = self.inner.compare_and_set_batch_at_ts(&batch)?;
                let cas_failures = cas_results.iter().filter(|&&r| !r).count();

                // Cache successfully rewritten entries
                for (succeeded, (_key, value, _old_ptr, new_ptr)) in
                    cas_results.iter().zip(&rewrites)
                {
                    if *succeeded && let Some(val) = value {
                        self.vlog.insert_cache(*new_ptr, val.clone());
                    }
                }

                self.vlog.schedule_deletion(analysis.file_id);
                if cas_failures == rewrites.len() {
                    self.vlog.schedule_deletion(new_file_id);
                }

                return Ok(GcResult {
                    old_file_id: analysis.file_id,
                    new_file_id,
                    keys_rewritten: rewrites.len() - cas_failures,
                    bytes_written: total_bytes,
                });
            }

            // Non-MVCC path: use user-key CAS
            let mut batch: Vec<CasEntry> = Vec::with_capacity(rewrites.len());
            for (key, _value, old_ptr, new_ptr) in &rewrites {
                let mut old_buf = Vec::with_capacity(1 + ValuePointer::encoded_size());
                old_buf.push(KvKind::ValuePointer as u8);
                old_ptr.encode(&mut old_buf);

                let mut new_buf = Vec::with_capacity(ValuePointer::encoded_size());
                new_ptr.encode(&mut new_buf);

                batch.push((
                    key.clone(),
                    old_buf,
                    KvKind::ValuePointer,
                    new_buf,
                    KvKind::ValuePointer,
                ));
            }
            let cas_results = self.inner.compare_and_set_batch_with_kind(&batch)?;
            let cas_failures = cas_results.iter().filter(|&&r| !r).count();

            // Cache successfully rewritten entries so subsequent reads avoid disk.
            for (succeeded, (_key, value, _old_ptr, new_ptr)) in cas_results.iter().zip(&rewrites) {
                if *succeeded && let Some(val) = value {
                    self.vlog.insert_cache(*new_ptr, val.clone());
                }
            }

            // Always schedule the old file for deletion. Concurrent writes during GC
            // go to the memtable (not the old vLog), so the old file has no live
            // entries after the CAS loop completes — even if some CAS operations
            // failed due to concurrent overwrites.
            self.vlog.schedule_deletion(analysis.file_id);
            if cas_failures == rewrites.len() {
                // All CAS operations failed — the new vLog file is entirely
                // unreferenced. Schedule it for immediate deletion to avoid leak.
                self.vlog.schedule_deletion(new_file_id);
            }

            Ok(GcResult {
                old_file_id: analysis.file_id,
                new_file_id,
                keys_rewritten: rewrites.len() - cas_failures,
                bytes_written: total_bytes,
            })
        })();

        match compact_res {
            std::result::Result::Ok(res) => {
                self.vlog
                    .record_gc_result(res.keys_rewritten, res.bytes_written);
                Ok(Some(res))
            }
            std::result::Result::Err(e) => {
                // Clean up the orphaned new vLog file and its index on error
                self.vlog.remove_index(new_file_id);
                let _ = std::fs::remove_file(&new_path);
                Err(e)
            }
        }
    }

    /// Run GC on a specific vLog file: analyze and compact if above threshold.
    pub fn gc_file(&self, file_id: u32) -> Result<Option<GcResult>> {
        if !self.vlog.try_acquire_gc_lock(file_id) {
            return Ok(None);
        }

        let result = (|| -> Result<Option<GcResult>> {
            let analysis = self.analyze_file(file_id)?;
            self.compact_file(&analysis)
        })();

        self.vlog.release_gc_lock(file_id);
        result
    }

    /// Run GC on all vLog files referenced by the current SST set.
    pub fn gc_all(&self) -> Result<Vec<GcResult>> {
        let snapshot = self.inner.state.load_full();
        let mut vlog_files: AHashSet<u32> = AHashSet::new();

        // Collect vLog file IDs from all SSTs
        for sst_id in snapshot.sstables.keys() {
            if let Some(refs) = self.vlog.get_sst_references(*sst_id) {
                vlog_files.extend(refs);
            }
        }

        let mut vlog_files: Vec<u32> = vlog_files.into_iter().collect();
        vlog_files.sort_unstable();

        let mut results = Vec::with_capacity(vlog_files.len());
        for file_id in vlog_files {
            if let Some(result) = self.gc_file(file_id)? {
                results.push(result);
            }
        }

        Ok(results)
    }
}
