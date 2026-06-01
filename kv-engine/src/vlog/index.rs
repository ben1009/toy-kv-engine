use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use anyhow::{Result, anyhow};
use bytes::{Buf, BufMut};

use crate::vlog::reader::ValueLogReader;

/// Magic number for vLog index files: "VIDX"
const INDEX_MAGIC: u32 = 0x56494458;

/// Header size for the index file (24 bytes).
const INDEX_HEADER_SIZE: usize = 24;

/// A single entry in the vLog index: maps a key to its location in the vLog file.
#[derive(Clone, Debug)]
pub struct VlogIndexEntry {
    /// Byte offset of the entry in the vLog file.
    pub offset: u64,
    /// The full key bytes.
    pub key: Vec<u8>,
    /// Length of the value in bytes (not stored in the index; read from vLog on demand).
    pub value_len: u32,
}

/// Per-file vLog index mapping keys to their vLog entry locations.
///
/// Stored as a companion `.vidx` file alongside each `.vlog` file.
/// Provides sequential entry iteration for GC liveness checks without reading
/// vLog headers.
#[derive(Clone, Debug)]
pub struct VlogIndex {
    /// All index entries in file order.
    entries: Vec<VlogIndexEntry>,
    /// The vLog file ID this index belongs to.
    file_id: u32,
}

impl VlogIndex {
    /// Create a new empty index for the given vLog file.
    pub fn new(file_id: u32) -> Self {
        Self {
            entries: Vec::new(),
            file_id,
        }
    }

    /// Add an entry to the index (used during building).
    pub fn add_entry(&mut self, offset: u64, key: Vec<u8>, value_len: u32) {
        self.entries.push(VlogIndexEntry {
            offset,
            key,
            value_len,
        });
    }

    /// Look up a key in the index (linear scan).
    /// This is primarily used in tests; the production GC path iterates
    /// `entries()` directly.
    pub fn lookup(&self, key: &[u8]) -> Option<&VlogIndexEntry> {
        self.entries.iter().find(|e| e.key == key)
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over all entries.
    pub fn entries(&self) -> &[VlogIndexEntry] {
        &self.entries
    }

    /// The vLog file ID this index belongs to.
    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    /// Rebuild the index by scanning vLog entry headers.
    /// Uses `iter_headers()` which reads only headers + keys (skipping values).
    pub fn rebuild_from_reader(reader: &ValueLogReader, file_id: u32) -> Result<Self> {
        let mut index = Self::new(file_id);
        let header_iter = reader.iter_headers()?.with_file_id(file_id);
        for meta_result in header_iter {
            let meta = meta_result?;
            index.add_entry(meta.ptr.offset, meta.key, meta.value_len);
        }
        Ok(index)
    }

    /// Load the index from a `.vidx` file, or rebuild it from the vLog if the
    /// index file is missing or corrupt.
    pub fn load_or_rebuild(
        index_path: &Path,
        vlog_reader: &ValueLogReader,
        file_id: u32,
    ) -> Result<Self> {
        match Self::load(index_path, file_id) {
            Ok(index) => Ok(index),
            Err(_) => Self::rebuild_from_reader(vlog_reader, file_id),
        }
    }

    /// Load the index from a `.vidx` file on disk.
    /// Returns an error if the file is missing or corrupt.
    pub fn load_from_file(path: &Path, file_id: u32) -> Result<Self> {
        Self::load(path, file_id)
    }

    /// Load the index from a `.vidx` file on disk.
    ///
    /// The CRC32 covers the header (excluding the CRC field itself) plus the
    /// entire entry payload, so any corruption in the header fields (including
    /// `entry_count`) is detected.
    fn load(path: &Path, expected_file_id: u32) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read header
        let mut hdr_buf = [0u8; INDEX_HEADER_SIZE];
        reader.read_exact(&mut hdr_buf)?;
        let mut hdr = &hdr_buf[..];

        let magic = hdr.get_u32_le();
        if magic != INDEX_MAGIC {
            return Err(anyhow!("vLog index magic mismatch: 0x{:08X}", magic));
        }
        let version = hdr.get_u16_le();
        if version != 1 {
            return Err(anyhow!("unsupported vLog index version: {}", version));
        }
        let entry_count = hdr.get_u32_le() as usize;
        let file_id = hdr.get_u32_le();
        if file_id != expected_file_id {
            return Err(anyhow!(
                "vLog index file_id mismatch: expected {}, got {}",
                expected_file_id,
                file_id
            ));
        }
        hdr.advance(6); // skip reserved bytes
        let stored_crc = hdr.get_u32_le();

        // Read all entries. Don't pre-allocate based on entry_count yet — a
        // corrupt header could claim u32::MAX entries and cause OOM. We validate
        // entry_count against the actual payload size below.
        let mut payload = Vec::new();
        reader.read_to_end(&mut payload)?;

        // Each entry is at least 14 bytes (8 offset + 2 key_len + 4 value_len).
        // Reject if entry_count is impossibly large for the payload.
        let max_possible = payload.len() / 14;
        if entry_count > max_possible {
            return Err(anyhow!(
                "vLog index entry_count {} exceeds maximum possible {} for payload size {}",
                entry_count,
                max_possible,
                payload.len()
            ));
        }

        let mut entries = Vec::with_capacity(entry_count);

        // Verify CRC over header (bytes 0..20, excluding CRC field) + payload
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&hdr_buf[..20]);
        hasher.update(&payload);
        let computed_crc = hasher.finalize();
        if computed_crc != stored_crc {
            return Err(anyhow!(
                "vLog index CRC mismatch: computed 0x{:08X}, stored 0x{:08X}",
                computed_crc,
                stored_crc
            ));
        }

        let mut pos = 0usize;
        for _ in 0..entry_count {
            // Entry layout: [offset:8][key_len:2][key:key_len][value_len:4]
            // Minimum size without key: 8 + 2 + 4 = 14
            if pos + 14 > payload.len() {
                return Err(anyhow!("vLog index truncated at entry {}", entries.len()));
            }
            let mut entry_bytes = &payload[pos..];
            let offset = entry_bytes.get_u64_le();
            let key_len = entry_bytes.get_u16_le() as usize;
            // Key starts at pos + 10 (after offset + key_len fields)
            let key_start = pos + 10;
            let key_end = key_start + key_len;
            let value_end = key_end + 4;
            if value_end > payload.len() {
                return Err(anyhow!(
                    "vLog index entry truncated at index {}",
                    entries.len()
                ));
            }
            let key = payload[key_start..key_end].to_vec();
            let value_len = (&payload[key_end..value_end]).get_u32_le();

            entries.push(VlogIndexEntry {
                offset,
                key,
                value_len,
            });
            pos = value_end;
        }

        // Validate that all payload bytes were consumed (catches corrupt entry_count)
        if pos != payload.len() {
            return Err(anyhow!(
                "vLog index payload not fully consumed: {} bytes parsed, {} bytes total",
                pos,
                payload.len()
            ));
        }

        Ok(Self { entries, file_id })
    }

    /// Persist the index to a `.vidx` file on disk.
    ///
    /// The CRC32 covers the header (excluding the CRC field itself) plus the
    /// entire entry payload.
    pub fn save(&self, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Pre-compute payload size to avoid reallocations (M4 fix)
        let payload_size: usize = self.entries.iter().map(|e| 8 + 2 + e.key.len() + 4).sum();

        // Serialize entries
        let mut payload = Vec::with_capacity(payload_size);
        for entry in &self.entries {
            payload.put_u64_le(entry.offset);
            payload.put_u16_le(entry.key.len() as u16);
            payload.put_slice(&entry.key);
            payload.put_u32_le(entry.value_len);
        }

        // Build header (24 bytes) with CRC placeholder
        let mut hdr = [0u8; INDEX_HEADER_SIZE];
        hdr[..4].copy_from_slice(&INDEX_MAGIC.to_le_bytes());
        hdr[4..6].copy_from_slice(&1u16.to_le_bytes());
        let entry_count: u32 = self.entries.len().try_into().map_err(|_| {
            anyhow!(
                "vLog index entry count {} exceeds u32::MAX",
                self.entries.len()
            )
        })?;
        hdr[6..10].copy_from_slice(&entry_count.to_le_bytes());
        hdr[10..14].copy_from_slice(&self.file_id.to_le_bytes());
        // hdr[14..20] reserved (zeros)

        // Compute CRC over header (excluding CRC field) + payload
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&hdr[..20]);
        hasher.update(&payload);
        let crc = hasher.finalize();
        hdr[20..24].copy_from_slice(&crc.to_le_bytes());

        writer.write_all(&hdr)?;
        writer.write_all(&payload)?;
        writer.flush()?;
        writer.get_ref().sync_data()?;

        Ok(())
    }
}

/// Path for the `.vidx` companion file given a `.vlog` file path.
pub fn index_path_for_vlog(vlog_path: &Path) -> std::path::PathBuf {
    vlog_path.with_extension("vidx")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vlog::builder::ValueLogWriter;

    fn write_test_vlog(
        path: &std::path::Path,
        file_id: u32,
    ) -> Vec<(&'static [u8], &'static [u8])> {
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2_value2"),
            (b"longer_key_name", b"short"),
            (b"k", b"another_value_here"),
        ];
        let mut writer = ValueLogWriter::create(path.to_path_buf(), file_id).unwrap();
        for (k, v) in &entries {
            writer.append(k, v).unwrap();
        }
        writer.close().unwrap();
        entries
    }

    #[test]
    fn test_index_add_and_lookup() {
        let mut index = VlogIndex::new(1);
        index.add_entry(16, b"key1".to_vec(), 6);
        index.add_entry(40, b"key2".to_vec(), 12);

        let entry = index.lookup(b"key1").unwrap();
        assert_eq!(entry.offset, 16);
        assert_eq!(entry.value_len, 6);

        let entry = index.lookup(b"key2").unwrap();
        assert_eq!(entry.offset, 40);
        assert_eq!(entry.value_len, 12);

        assert!(index.lookup(b"missing").is_none());
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn test_index_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = VlogIndex::new(42);
        index.add_entry(16, b"alpha".to_vec(), 3);
        index.add_entry(32, b"beta".to_vec(), 6);
        index.add_entry(48, b"gamma".to_vec(), 1);

        let idx_path = dir.path().join("42.vidx");
        index.save(&idx_path).unwrap();

        let loaded = VlogIndex::load(&idx_path, 42).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.file_id(), 42);

        let e = loaded.lookup(b"alpha").unwrap();
        assert_eq!(e.offset, 16);
        assert_eq!(e.value_len, 3);

        let e = loaded.lookup(b"beta").unwrap();
        assert_eq!(e.offset, 32);

        let e = loaded.lookup(b"gamma").unwrap();
        assert_eq!(e.offset, 48);
        assert_eq!(e.value_len, 1);
    }

    #[test]
    fn test_index_rebuild_from_reader() {
        let dir = tempfile::tempdir().unwrap();
        let vlog_path = dir.path().join("7.vlog");
        let file_id = 7u32;

        write_test_vlog(&vlog_path, file_id);

        let reader = ValueLogReader::open(vlog_path)
            .unwrap()
            .with_file_id(file_id);
        let index = VlogIndex::rebuild_from_reader(&reader, file_id).unwrap();

        assert_eq!(index.len(), 4);
        assert!(index.lookup(b"key1").is_some());
        assert!(index.lookup(b"key2").is_some());
        assert!(index.lookup(b"longer_key_name").is_some());
        assert!(index.lookup(b"k").is_some());
        assert!(index.lookup(b"missing").is_none());
    }

    #[test]
    fn test_index_load_or_rebuild_loads_existing() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = VlogIndex::new(5);
        index.add_entry(16, b"hello".to_vec(), 5);

        let idx_path = dir.path().join("5.vidx");
        index.save(&idx_path).unwrap();

        // Create a dummy vlog (won't be used since index file exists)
        let vlog_path = dir.path().join("5.vlog");
        write_test_vlog(&vlog_path, 5);

        let reader = ValueLogReader::open(vlog_path).unwrap().with_file_id(5);
        let loaded = VlogIndex::load_or_rebuild(&idx_path, &reader, 5).unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.lookup(b"hello").is_some());
    }

    #[test]
    fn test_index_load_or_rebuild_rebuilds_missing() {
        let dir = tempfile::tempdir().unwrap();
        let vlog_path = dir.path().join("9.vlog");
        let file_id = 9u32;

        write_test_vlog(&vlog_path, file_id);

        let idx_path = dir.path().join("9.vidx"); // doesn't exist
        let reader = ValueLogReader::open(vlog_path)
            .unwrap()
            .with_file_id(file_id);
        let index = VlogIndex::load_or_rebuild(&idx_path, &reader, file_id).unwrap();

        assert_eq!(index.len(), 4);
        assert!(index.lookup(b"key1").is_some());
    }

    #[test]
    fn test_index_crc_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = VlogIndex::new(1);
        index.add_entry(16, b"key".to_vec(), 3);

        let idx_path = dir.path().join("1.vidx");
        index.save(&idx_path).unwrap();

        // Corrupt the payload
        let mut data = std::fs::read(&idx_path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&idx_path, &data).unwrap();

        let result = VlogIndex::load(&idx_path, 1);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("CRC"), "expected CRC error, got: {}", msg);
    }

    #[test]
    fn test_index_crc_detects_header_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = VlogIndex::new(1);
        index.add_entry(16, b"key1".to_vec(), 3);
        index.add_entry(32, b"key2".to_vec(), 6);

        let idx_path = dir.path().join("1.vidx");
        index.save(&idx_path).unwrap();

        // Corrupt the entry_count in the header (bytes 6..10) to a smaller value
        let mut data = std::fs::read(&idx_path).unwrap();
        data[6] = 1; // entry_count = 1 instead of 2
        std::fs::write(&idx_path, &data).unwrap();

        // The CRC should fail because it now covers the header
        let result = VlogIndex::load(&idx_path, 1);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("CRC") || msg.contains("not fully consumed"),
            "expected CRC or payload mismatch error, got: {}",
            msg
        );
    }

    #[test]
    fn test_index_empty() {
        let index = VlogIndex::new(0);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(index.lookup(b"any").is_none());
    }
}
