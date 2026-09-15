//! Checksummed PITR v5 WAL seal sidecars.
//!
//! A seal records the exact logical WAL prefix and the ordered commit-time
//! index derived from its batches. The WAL remains independently recoverable;
//! the sidecar is only accepted when it binds to the supplied v5 header and
//! source digest.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

use crate::pitr::{ChainAnchor, RecordedAt, WAL_V5_ALIGNMENT, WAL_V5_HEADER_LEN, WalV5Header};

const MAGIC: &[u8; 8] = b"TKVSEAL1";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 8 + 2 + 2 + 16 + 16 + 8 + 32 + 8 + 8 + 8 + 8 + 4;
const ENTRY_BYTES: usize = 8 + 8 + 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SealEntry {
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct V5Seal {
    pub(crate) header: WalV5Header,
    pub(crate) wal_digest: [u8; 32],
    pub(crate) logical_length: u64,
    pub(crate) entries: Vec<SealEntry>,
}

impl V5Seal {
    pub(crate) fn first_commit_ts(&self) -> Option<u64> {
        self.entries.first().map(|entry| entry.commit_ts)
    }

    pub(crate) fn last_commit_ts(&self) -> Option<u64> {
        self.entries.last().map(|entry| entry.commit_ts)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let count = u64::try_from(self.entries.len())?;
        let body_len = count
            .checked_mul(ENTRY_BYTES as u64)
            .ok_or_else(|| anyhow::anyhow!("PITR seal entry length overflow"))?;
        let total = HEADER_BYTES
            .checked_add(usize::try_from(body_len)?)
            .ok_or_else(|| anyhow::anyhow!("PITR seal length overflow"))?;
        let mut output = vec![0u8; total];
        output[..8].copy_from_slice(MAGIC);
        output[8..10].copy_from_slice(&VERSION.to_be_bytes());
        output[10..12].copy_from_slice(&(HEADER_BYTES as u16).to_be_bytes());
        output[12..28].copy_from_slice(&self.header.timeline_id.0);
        output[28..44].copy_from_slice(&self.header.archive_epoch_id.0);
        output[44..52].copy_from_slice(&self.header.segment_id.0.to_be_bytes());
        output[52..84].copy_from_slice(&self.wal_digest);
        output[84..92].copy_from_slice(&self.logical_length.to_be_bytes());
        output[92..100].copy_from_slice(&count.to_be_bytes());
        let first = self.first_commit_ts().unwrap_or(0);
        let last = self.last_commit_ts().unwrap_or(0);
        output[100..108].copy_from_slice(&first.to_be_bytes());
        output[108..116].copy_from_slice(&last.to_be_bytes());
        let header_crc = crc32fast::hash(&output[..116]);
        output[116..120].copy_from_slice(&header_crc.to_be_bytes());
        let mut offset = HEADER_BYTES;
        for entry in &self.entries {
            output[offset..offset + 8].copy_from_slice(&entry.commit_ts.to_be_bytes());
            output[offset + 8..offset + 16].copy_from_slice(&entry.recorded_at.secs.to_be_bytes());
            output[offset + 16..offset + 20]
                .copy_from_slice(&entry.recorded_at.nanos.to_be_bytes());
            offset += ENTRY_BYTES;
        }
        Ok(output)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() >= HEADER_BYTES, "PITR seal is truncated");
        ensure!(&bytes[..8] == MAGIC, "invalid PITR seal magic");
        ensure!(
            u16::from_be_bytes(bytes[8..10].try_into()?) == VERSION,
            "unsupported PITR seal version"
        );
        ensure!(
            u16::from_be_bytes(bytes[10..12].try_into()?) as usize == HEADER_BYTES,
            "invalid PITR seal header length"
        );
        ensure!(
            crc32fast::hash(&bytes[..116]) == u32::from_be_bytes(bytes[116..120].try_into()?),
            "PITR seal header checksum mismatch"
        );
        let count = usize::try_from(u64::from_be_bytes(bytes[92..100].try_into()?))?;
        let body_len = count
            .checked_mul(ENTRY_BYTES)
            .ok_or_else(|| anyhow::anyhow!("PITR seal entry length overflow"))?;
        ensure!(
            bytes.len() == HEADER_BYTES + body_len,
            "PITR seal length mismatch"
        );
        let timeline_id = crate::pitr::TimelineId(bytes[12..28].try_into()?);
        let archive_epoch_id = crate::pitr::ArchiveEpochId(bytes[28..44].try_into()?);
        let segment_id = crate::pitr::SegmentId(u64::from_be_bytes(bytes[44..52].try_into()?));
        let header = WalV5Header {
            timeline_id,
            archive_epoch_id,
            segment_id,
            predecessor: ChainAnchor::Genesis { archive_epoch_id },
        };
        let logical_length = u64::from_be_bytes(bytes[84..92].try_into()?);
        let mut entries = Vec::with_capacity(count);
        let mut offset = HEADER_BYTES;
        for _ in 0..count {
            entries.push(SealEntry {
                commit_ts: u64::from_be_bytes(bytes[offset..offset + 8].try_into()?),
                recorded_at: RecordedAt {
                    secs: i64::from_be_bytes(bytes[offset + 8..offset + 16].try_into()?),
                    nanos: u32::from_be_bytes(bytes[offset + 16..offset + 20].try_into()?),
                },
            });
            offset += ENTRY_BYTES;
        }
        let seal = Self {
            header,
            wal_digest: bytes[52..84].try_into()?,
            logical_length,
            entries,
        };
        seal.validate()?;
        Ok(seal)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.logical_length >= WAL_V5_HEADER_LEN as u64,
            "PITR seal logical length is below header"
        );
        ensure!(
            self.logical_length.is_multiple_of(WAL_V5_ALIGNMENT as u64),
            "PITR seal logical length is unaligned"
        );
        ensure!(self.wal_digest != [0; 32], "PITR seal WAL digest is empty");
        let mut previous = 0;
        for entry in &self.entries {
            ensure!(entry.commit_ts != 0, "PITR seal commit timestamp is zero");
            ensure!(
                entry.recorded_at.nanos < 1_000_000_000,
                "PITR seal recorded time is invalid"
            );
            ensure!(
                previous < entry.commit_ts,
                "PITR seal commit timestamps are not increasing"
            );
            previous = entry.commit_ts;
        }
        ensure!(
            (self.entries.is_empty() && self.logical_length == WAL_V5_HEADER_LEN as u64)
                || (!self.entries.is_empty() && self.logical_length > WAL_V5_HEADER_LEN as u64),
            "PITR seal empty/nonempty boundary is invalid"
        );
        Ok(())
    }
}

pub(crate) fn build_v5_seal(wal: &[u8]) -> Result<(V5Seal, Vec<u8>)> {
    let header = crate::pitr::decode_v5_file_header(wal)?;
    ensure!(wal.len() >= WAL_V5_HEADER_LEN, "PITR WAL is truncated");
    let mut offset = WAL_V5_HEADER_LEN;
    let mut entries = Vec::new();
    while offset < wal.len() {
        if wal[offset..].iter().all(|byte| *byte == 0) {
            break;
        }
        let decoded = crate::pitr::decode_v5_batch(wal, offset, crate::pitr::LIVE_WAL_V5_LIMITS)?;
        entries.push(SealEntry {
            commit_ts: decoded.batch.commit_ts,
            recorded_at: decoded.batch.recorded_at,
        });
        offset = decoded.logical_end;
    }
    let logical_length = offset as u64;
    let wal_digest = Sha256::digest(&wal[..offset]).into();
    let seal = V5Seal {
        header,
        wal_digest,
        logical_length,
        entries,
    };
    let bytes = seal.encode()?;
    Ok((seal, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_header_seal_round_trips() {
        let header = WalV5Header {
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(3),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
        };
        let seal = V5Seal {
            header,
            wal_digest: [4; 32],
            logical_length: WAL_V5_HEADER_LEN as u64,
            entries: Vec::new(),
        };
        assert_eq!(V5Seal::decode(&seal.encode().unwrap()).unwrap(), seal);
    }

    #[test]
    fn build_seal_indexes_v5_batches_and_logical_prefix() {
        let header = WalV5Header {
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(3),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
        };
        let batch = crate::pitr::WalBatch {
            commit_ts: 7,
            recorded_at: RecordedAt {
                secs: 11,
                nanos: 12,
            },
            entries: vec![crate::pitr::WalEntry::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            }],
        };
        let mut wal = crate::pitr::encode_v5_file_header(header).unwrap().to_vec();
        wal.extend_from_slice(
            &crate::pitr::encode_v5_batch(&batch, crate::pitr::LIVE_WAL_V5_LIMITS).unwrap(),
        );
        let (seal, bytes) = build_v5_seal(&wal).unwrap();
        assert_eq!(seal.first_commit_ts(), Some(7));
        assert_eq!(seal.last_commit_ts(), Some(7));
        assert_eq!(seal.entries.len(), 1);
        assert_eq!(V5Seal::decode(&bytes).unwrap(), seal);
    }
}
