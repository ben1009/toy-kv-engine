//! Incremental-backup repository catalog primitives (RFC 022).
//!
//! The catalog is intentionally append-only. A frame is length-delimited and
//! checksummed, so recovery can discard only a torn final write while treating
//! complete semantic corruption as an error.

#![allow(dead_code)] // Wired to repository publication in the next RFC 022 slice.

use std::io::{Read, Write};

use anyhow::{Context, Result, anyhow, ensure};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_CATALOG_FRAME_BYTES: usize = 1024 * 1024;
const CATALOG_FRAME_HEADER_BYTES: usize = 12;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_CATALOG_RECORDS: usize = 1_000_000;
const CATALOG_FORMAT_VERSION: u8 = 1;

pub(crate) struct CatalogFrames {
    pub(crate) frames: Vec<CatalogFrame>,
    pub(crate) last_complete_offset: u64,
    pub(crate) torn_tail: bool,
}

pub(crate) struct CatalogFrame {
    pub(crate) record: CatalogRecord,
    /// Exact validated bytes from the catalog, used for Commit/Prepare binding.
    pub(crate) payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    version: u8,
    record: CatalogRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CatalogRecord {
    HighWater {
        sequence: u64,
        allocated_id: u64,
    },
    Prepare {
        sequence: u64,
        id: u64,
        parent_id: Option<u64>,
        generation_checksum: [u8; 32],
    },
    Commit {
        sequence: u64,
        id: u64,
        prepare_sequence: u64,
        prepare_digest: [u8; 32],
    },
}

pub(crate) fn append_catalog_record(file: &mut impl Write, record: &CatalogRecord) -> Result<()> {
    let payload = encode_catalog_payload(record)?;
    ensure!(
        payload.len() <= MAX_CATALOG_FRAME_BYTES,
        "backup catalog record exceeds frame limit"
    );
    let length =
        u32::try_from(payload.len()).map_err(|_| anyhow!("backup catalog record too large"))?;
    let checksum = crc32(&payload);
    let mut header = [0_u8; CATALOG_FRAME_HEADER_BYTES];
    header[..4].copy_from_slice(&length.to_le_bytes());
    header[4..8].copy_from_slice(&checksum.to_le_bytes());
    let header_checksum = crc32(&header[..8]);
    header[8..].copy_from_slice(&header_checksum.to_le_bytes());
    file.write_all(&header)?;
    file.write_all(&payload)?;
    Ok(())
}

fn encode_catalog_payload(record: &CatalogRecord) -> Result<Vec<u8>> {
    serde_json::to_vec(&WireRecord {
        version: CATALOG_FORMAT_VERSION,
        record: record.clone(),
    })
    .context("failed to encode backup catalog record")
}

pub(crate) fn prepare_payload_digest(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

/// Reads bounded, valid complete frames. A torn tail retains the exact byte
/// offset to which a lock-holding recovery path may truncate.
pub(crate) fn read_catalog_records(mut file: impl Read) -> Result<CatalogFrames> {
    let mut bytes = Vec::new();
    file.by_ref()
        .take((MAX_CATALOG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_CATALOG_BYTES,
        "backup catalog exceeds size limit"
    );
    let mut frames = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        if bytes.len() - offset < CATALOG_FRAME_HEADER_BYTES {
            return Ok(CatalogFrames {
                frames,
                last_complete_offset: offset as u64,
                torn_tail: true,
            });
        }
        let header = &bytes[offset..offset + CATALOG_FRAME_HEADER_BYTES];
        ensure!(
            crc32(&header[..8]) == u32::from_le_bytes(header[8..].try_into().unwrap()),
            "backup catalog frame header checksum mismatch"
        );
        let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        ensure!(
            length <= MAX_CATALOG_FRAME_BYTES,
            "backup catalog frame exceeds limit"
        );
        ensure!(
            frames.len() < MAX_CATALOG_RECORDS,
            "backup catalog has too many records"
        );
        let expected_checksum = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let end = offset
            .checked_add(CATALOG_FRAME_HEADER_BYTES + length)
            .ok_or_else(|| anyhow!("backup catalog frame overflow"))?;
        if end > bytes.len() {
            return Ok(CatalogFrames {
                frames,
                last_complete_offset: offset as u64,
                torn_tail: true,
            });
        }
        let payload = &bytes[offset + CATALOG_FRAME_HEADER_BYTES..end];
        ensure!(
            crc32(payload) == expected_checksum,
            "backup catalog frame checksum mismatch"
        );
        let wire: WireRecord =
            serde_json::from_slice(payload).context("invalid backup catalog record")?;
        ensure!(
            wire.version == CATALOG_FORMAT_VERSION,
            "unsupported backup catalog format version"
        );
        ensure!(
            payload == encode_catalog_payload(&wire.record)?,
            "backup catalog record is not canonically encoded"
        );
        frames.push(CatalogFrame {
            record: wire.record,
            payload: payload.to_vec(),
        });
        offset = end;
    }
    Ok(CatalogFrames {
        frames,
        last_complete_offset: offset as u64,
        torn_tail: false,
    })
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_round_trip_and_torn_tail() {
        let first = CatalogRecord::HighWater {
            sequence: 1,
            allocated_id: 1,
        };
        let second = CatalogRecord::Prepare {
            sequence: 2,
            id: 1,
            parent_id: None,
            generation_checksum: [7; 32],
        };
        let mut bytes = Vec::new();
        append_catalog_record(&mut bytes, &first).unwrap();
        append_catalog_record(&mut bytes, &second).unwrap();
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        assert_eq!(
            frames
                .frames
                .iter()
                .map(|frame| &frame.record)
                .collect::<Vec<_>>(),
            vec![&first, &second]
        );
        assert!(!frames.torn_tail);
        assert_eq!(frames.last_complete_offset as usize, bytes.len());

        bytes.pop();
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        assert_eq!(
            frames
                .frames
                .iter()
                .map(|frame| &frame.record)
                .collect::<Vec<_>>(),
            vec![&first]
        );
        assert!(frames.torn_tail);
    }

    #[test]
    fn catalog_rejects_checksum_mismatch() {
        let mut bytes = Vec::new();
        append_catalog_record(
            &mut bytes,
            &CatalogRecord::HighWater {
                sequence: 1,
                allocated_id: 1,
            },
        )
        .unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        assert!(read_catalog_records(bytes.as_slice()).is_err());
    }

    #[test]
    fn catalog_rejects_corrupt_header_and_noncanonical_payload() {
        let record = CatalogRecord::HighWater {
            sequence: 1,
            allocated_id: 1,
        };
        let mut bytes = Vec::new();
        append_catalog_record(&mut bytes, &record).unwrap();
        bytes[0] ^= 1;
        assert!(read_catalog_records(bytes.as_slice()).is_err());

        let noncanonical = br#"{\"version\":1, \"record\":{\"type\":\"high_water\",\"sequence\":1,\"allocated_id\":1}}"#;
        let mut framed = Vec::new();
        let mut header = [0_u8; CATALOG_FRAME_HEADER_BYTES];
        header[..4].copy_from_slice(&(noncanonical.len() as u32).to_le_bytes());
        header[4..8].copy_from_slice(&crc32(noncanonical).to_le_bytes());
        let header_checksum = crc32(&header[..8]);
        header[8..].copy_from_slice(&header_checksum.to_le_bytes());
        framed.extend_from_slice(&header);
        framed.extend_from_slice(noncanonical);
        assert!(read_catalog_records(framed.as_slice()).is_err());
    }

    #[test]
    fn prepare_digest_uses_exact_persisted_payload() {
        let prepare = CatalogRecord::Prepare {
            sequence: 2,
            id: 1,
            parent_id: None,
            generation_checksum: [9; 32],
        };
        let mut bytes = Vec::new();
        append_catalog_record(&mut bytes, &prepare).unwrap();
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        let frame = &frames.frames[0];
        assert_eq!(frame.record, prepare);
        let expected: [u8; 32] = Sha256::digest(&frame.payload).into();
        assert_eq!(prepare_payload_digest(&frame.payload), expected);
    }
}
