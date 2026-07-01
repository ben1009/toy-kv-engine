//! Control log for chaos-testing.
//!
//! The control log is a separate, independently-fsynced file outside the database
//! directory. It records operation intents and durability boundaries so the parent
//! harness can distinguish operations that were externally committed at crash time
//! from those that were not — without relying on the database under test.

use serde::{Deserialize, Serialize};
use std::io::Write;

/// A single record in the control log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlLogRecord {
    pub op_id: u64,
    pub kind: OperationKind,
    pub checkpoint: Checkpoint,
    pub ts_ns: u128,
}

/// The kind of an operation being performed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationKind {
    /// Insert or update a single key.
    Put {
        key: String,
        /// The actual value (as a string; for Phase 1 all values are printable ASCII).
        value: String,
    },
    /// Delete a single key.
    Delete { key: String },
    /// Delete a range of keys.
    DeleteRange { start: String, end: String },
    /// Issue a write_batch of records.
    WriteBatch { entries: Vec<BatchEntry> },
    /// Force a memtable flush.
    Flush,
    /// Force a full compaction.
    FullCompaction,
    /// A sync-point marker for the parent harness to detect.
    SyncPoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEntry {
    pub kind: BatchOpKind,
    pub key: String,
    /// Value bytes as a string (None for deletes; Phase 1 values are printable ASCII).
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BatchOpKind {
    Put,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Checkpoint {
    /// The operation was started (intent). May or may not be durable.
    IntentStarted,
    /// The operation crossed the external durability boundary.
    DurabilityBoundaryPassed,
}

/// Writer for the control log.
///
/// Appends JSON-lines records to a file. Only `write_durability_boundary()` calls
/// `sync_all()` — intents are written without fsync so they may be lost on crash,
/// faithfully reflecting the durability contract.
pub struct ControlLogWriter {
    file: std::fs::File,
    path: std::path::PathBuf,
    next_op_id: u64,
}

impl ControlLogWriter {
    /// Open (or create) the control log at `path`.
    pub fn new(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            file,
            path,
            next_op_id: 0,
        })
    }

    /// Allocate the next operation id.
    pub fn next_op_id(&mut self) -> u64 {
        let id = self.next_op_id;
        self.next_op_id += 1;
        id
    }

    /// Write an intent record (not fsynced).
    pub fn write_intent(&mut self, op_id: u64, kind: OperationKind) -> std::io::Result<()> {
        let record = ControlLogRecord {
            op_id,
            kind,
            checkpoint: Checkpoint::IntentStarted,
            ts_ns: now_ns(),
        };
        let line = serde_json::to_string(&record).map_err(std::io::Error::other)?;
        let mut line_bytes = line.into_bytes();
        line_bytes.push(b'\n');
        self.file.write_all(&line_bytes)?;
        Ok(())
    }

    /// Write a durability-boundary record and fsync.
    pub fn write_durability_boundary(
        &mut self,
        op_id: u64,
        kind: OperationKind,
    ) -> std::io::Result<()> {
        let record = ControlLogRecord {
            op_id,
            kind,
            checkpoint: Checkpoint::DurabilityBoundaryPassed,
            ts_ns: now_ns(),
        };
        let line = serde_json::to_string(&record).map_err(std::io::Error::other)?;
        let mut line_bytes = line.into_bytes();
        line_bytes.push(b'\n');
        self.file.write_all(&line_bytes)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Write a sync-point marker and fsync.
    /// The parent harness detects this to know when to send SIGKILL.
    pub fn write_sync_point(&mut self) -> std::io::Result<()> {
        let id = self.next_op_id();
        self.write_durability_boundary(id, OperationKind::SyncPoint)
    }

    /// Consume the writer and return the path to the log file.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

/// Read and classify operations from a control log.
pub struct ControlLogReader {
    records: Vec<ControlLogRecord>,
}

impl ControlLogReader {
    /// Parse the control log file at `path`.
    ///
    /// Tolerates a torn final line (incomplete JSON on the last line when the file
    /// does not end with `\n`) since the child process may be killed between `write`
    /// and the implicit newline. All other parse failures are surfaced as errors.
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        // Read raw bytes to tolerate a torn multi-byte UTF-8 character if the
        // child was SIGKILL'd mid-write. We validate UTF-8 per line instead.
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        let mut records = Vec::new();
        let ends_with_newline = bytes.last().copied() == Some(b'\n');
        let mut lines = bytes.split(|&b| b == b'\n').peekable();
        while let Some(line_bytes) = lines.next() {
            if line_bytes.iter().all(|&b| b.is_ascii_whitespace()) {
                continue;
            }
            let line = match std::str::from_utf8(line_bytes) {
                Ok(s) => s,
                // Tolerate torn multi-byte UTF-8 on the final line.
                Err(_) if lines.peek().is_none() && !ends_with_newline => break,
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "invalid UTF-8 in control log at line {}: {e}",
                            records.len() + 1
                        ),
                    ));
                }
            };
            match serde_json::from_str::<ControlLogRecord>(line) {
                Ok(record) => records.push(record),
                // Tolerate a torn final line: the child may have been killed between
                // write() and the newline. Only the LAST line gets this leniency.
                Err(_) if lines.peek().is_none() && !ends_with_newline => break,
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("corrupt control log at line {}: {e}", records.len() + 1),
                    ));
                }
            }
        }
        Ok(Self { records })
    }

    /// Return all parsed records.
    pub fn records(&self) -> &[ControlLogRecord] {
        &self.records
    }

    /// Classify operations: returns `(committed_op_ids, possibly_visible_op_ids)`.
    ///
    /// - An operation is **committed** if it has at least one DurabilityBoundaryPassed marker.
    /// - An operation is **possibly visible** if it has only IntentStarted markers (it may or may
    ///   not have been durable before the crash, due to the durable-before-ack window).
    pub fn classify_visibility(&self) -> (Vec<u64>, Vec<u64>) {
        let mut committed = Vec::new();
        let mut possibly_visible = Vec::new();

        // Track per-op_id the highest checkpoint seen
        use std::collections::BTreeMap;
        let mut ops: BTreeMap<u64, bool> = BTreeMap::new();
        for rec in &self.records {
            match rec.checkpoint {
                Checkpoint::DurabilityBoundaryPassed => {
                    ops.insert(rec.op_id, true);
                }
                Checkpoint::IntentStarted => {
                    ops.entry(rec.op_id).or_insert(false);
                }
            }
        }
        for (op_id, durable) in ops {
            if durable {
                committed.push(op_id);
            } else {
                possibly_visible.push(op_id);
            }
        }
        (committed, possibly_visible)
    }

    /// Return committed and possibly-visible records grouped by op_id.
    pub fn committed_records(&self, committed_ids: &[u64]) -> Vec<&ControlLogRecord> {
        let set: std::collections::BTreeSet<u64> = committed_ids.iter().copied().collect();
        self.records
            .iter()
            .filter(|r| set.contains(&r.op_id))
            .collect()
    }
}

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_log_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.log");

        let mut writer = ControlLogWriter::new(&path).unwrap();
        let op1 = writer.next_op_id();
        writer
            .write_intent(
                op1,
                OperationKind::Put {
                    key: "k1".into(),
                    value: "test".into(),
                },
            )
            .unwrap();
        writer
            .write_durability_boundary(
                op1,
                OperationKind::Put {
                    key: "k1".into(),
                    value: "test".into(),
                },
            )
            .unwrap();
        let op2 = writer.next_op_id();
        writer
            .write_intent(op2, OperationKind::Delete { key: "k2".into() })
            .unwrap();
        writer.write_sync_point().unwrap();
        drop(writer);

        let reader = ControlLogReader::open(&path).unwrap();
        assert_eq!(reader.records().len(), 4);

        let (committed, possibly_visible) = reader.classify_visibility();
        assert_eq!(committed.len(), 2); // op1 + sync point
        assert_eq!(possibly_visible.len(), 1); // op2 (intent only)
    }

    #[test]
    fn test_control_log_truncated_last_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.log");

        // Write a valid record, then append a truncated line
        let mut writer = ControlLogWriter::new(&path).unwrap();
        let op1 = writer.next_op_id();
        writer
            .write_durability_boundary(
                op1,
                OperationKind::Put {
                    key: "k1".into(),
                    value: "test".into(),
                },
            )
            .unwrap();
        drop(writer);

        // Append a partial JSON line
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        write!(f, "{{\"partial\"").unwrap();
        drop(f);

        let reader = ControlLogReader::open(&path).unwrap();
        assert_eq!(reader.records().len(), 1);
        assert_eq!(reader.records()[0].op_id, op1);
    }

    #[test]
    fn test_control_log_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.log");
        std::fs::write(&path, b"").unwrap();
        let reader = ControlLogReader::open(&path).unwrap();
        assert_eq!(reader.records().len(), 0);
        let (committed, possibly_visible) = reader.classify_visibility();
        assert!(committed.is_empty());
        assert!(possibly_visible.is_empty());
    }

    #[test]
    fn test_control_log_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.log");
        let reader = ControlLogReader::open(&path).unwrap();
        assert_eq!(reader.records().len(), 0);
    }
}
