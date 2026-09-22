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
const HEADER_BYTES: usize = 224;
const ENTRY_BYTES: usize = 24;

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
        output[12..14].copy_from_slice(&crate::pitr::WAL_V5_VERSION.to_be_bytes());
        output[16..32].copy_from_slice(&self.header.timeline_id.0);
        output[32..48].copy_from_slice(&self.header.archive_epoch_id.0);
        output[48..56].copy_from_slice(&self.header.segment_id.0.to_be_bytes());
        match self.header.predecessor {
            ChainAnchor::Genesis { archive_epoch_id } => ensure!(
                archive_epoch_id == self.header.archive_epoch_id,
                "PITR seal genesis predecessor epoch mismatch"
            ),
            ChainAnchor::Segment(anchor) => {
                output[56] = 1;
                output[64..72].copy_from_slice(&anchor.segment_id.0.to_be_bytes());
                output[72..104].copy_from_slice(&anchor.wal_digest);
                output[104..136].copy_from_slice(&anchor.seal_digest);
            }
        }
        output[136..144].copy_from_slice(&self.logical_length.to_be_bytes());
        output[144..152].copy_from_slice(&count.to_be_bytes());
        let first = self.first_commit_ts().unwrap_or(0);
        let last = self.last_commit_ts().unwrap_or(0);
        output[152..160].copy_from_slice(&first.to_be_bytes());
        output[160..168].copy_from_slice(&last.to_be_bytes());
        output[168..200].copy_from_slice(&self.wal_digest);
        output[200..208].copy_from_slice(&body_len.to_be_bytes());
        let mut offset = HEADER_BYTES;
        for entry in &self.entries {
            output[offset..offset + 8].copy_from_slice(&entry.commit_ts.to_be_bytes());
            output[offset + 8..offset + 16].copy_from_slice(&entry.recorded_at.secs.to_be_bytes());
            output[offset + 16..offset + 20]
                .copy_from_slice(&entry.recorded_at.nanos.to_be_bytes());
            offset += ENTRY_BYTES;
        }
        let index_crc = crc32fast::hash(&output[HEADER_BYTES..]);
        output[208..212].copy_from_slice(&index_crc.to_be_bytes());
        let header_crc = crc32fast::hash(&output[8..212]);
        output[212..216].copy_from_slice(&header_crc.to_be_bytes());
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
            u16::from_be_bytes(bytes[12..14].try_into()?) == crate::pitr::WAL_V5_VERSION,
            "invalid PITR seal WAL format"
        );
        ensure!(
            bytes[14..16].iter().all(|byte| *byte == 0)
                && bytes[57..64].iter().all(|byte| *byte == 0)
                && bytes[216..224].iter().all(|byte| *byte == 0),
            "nonzero PITR seal reserved field"
        );
        ensure!(
            crc32fast::hash(&bytes[8..212]) == u32::from_be_bytes(bytes[212..216].try_into()?),
            "PITR seal header checksum mismatch"
        );
        let count = usize::try_from(u64::from_be_bytes(bytes[144..152].try_into()?))?;
        let body_len = count
            .checked_mul(ENTRY_BYTES)
            .ok_or_else(|| anyhow::anyhow!("PITR seal entry length overflow"))?;
        ensure!(
            u64::from_be_bytes(bytes[200..208].try_into()?) == body_len as u64,
            "PITR seal index length mismatch"
        );
        ensure!(
            bytes.len() == HEADER_BYTES + body_len,
            "PITR seal length mismatch"
        );
        ensure!(
            crc32fast::hash(&bytes[HEADER_BYTES..])
                == u32::from_be_bytes(bytes[208..212].try_into()?),
            "PITR seal index checksum mismatch"
        );
        let timeline_id = crate::pitr::TimelineId(bytes[16..32].try_into()?);
        let archive_epoch_id = crate::pitr::ArchiveEpochId(bytes[32..48].try_into()?);
        let segment_id = crate::pitr::SegmentId(u64::from_be_bytes(bytes[48..56].try_into()?));
        let predecessor = match bytes[56] {
            0 => {
                ensure!(
                    bytes[64..136].iter().all(|byte| *byte == 0),
                    "nonzero genesis predecessor fields"
                );
                ChainAnchor::Genesis { archive_epoch_id }
            }
            1 => ChainAnchor::Segment(crate::pitr::SegmentAnchor {
                segment_id: crate::pitr::SegmentId(u64::from_be_bytes(bytes[64..72].try_into()?)),
                wal_digest: bytes[72..104].try_into()?,
                seal_digest: bytes[104..136].try_into()?,
            }),
            _ => anyhow::bail!("unknown PITR seal predecessor kind"),
        };
        let header = WalV5Header {
            timeline_id,
            archive_epoch_id,
            segment_id,
            predecessor,
        };
        let logical_length = u64::from_be_bytes(bytes[136..144].try_into()?);
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
            ensure!(
                bytes[offset + 20..offset + 24]
                    .iter()
                    .all(|byte| *byte == 0),
                "nonzero PITR seal index reserved field"
            );
            offset += ENTRY_BYTES;
        }
        ensure!(
            u64::from_be_bytes(bytes[152..160].try_into()?)
                == entries.first().map_or(0, |entry| entry.commit_ts)
                && u64::from_be_bytes(bytes[160..168].try_into()?)
                    == entries.last().map_or(0, |entry| entry.commit_ts),
            "PITR seal commit range mismatch"
        );
        let seal = Self {
            header,
            wal_digest: bytes[168..200].try_into()?,
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
        match self.header.predecessor {
            ChainAnchor::Genesis { archive_epoch_id } => ensure!(
                archive_epoch_id == self.header.archive_epoch_id,
                "PITR seal genesis predecessor epoch mismatch"
            ),
            ChainAnchor::Segment(anchor) => ensure!(
                anchor.wal_digest != [0; 32] && anchor.seal_digest != [0; 32],
                "PITR seal predecessor digest is empty"
            ),
        }
        let mut previous = 0;
        let mut previous_recorded_at = None;
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
            ensure!(
                previous_recorded_at.is_none_or(|previous| entry.recorded_at >= previous),
                "PITR seal recorded times are decreasing"
            );
            previous = entry.commit_ts;
            previous_recorded_at = Some(entry.recorded_at);
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
    fn segment_predecessor_round_trips() {
        let seal = V5Seal {
            header: WalV5Header {
                timeline_id: crate::pitr::TimelineId([1; 16]),
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
                segment_id: crate::pitr::SegmentId(4),
                predecessor: ChainAnchor::Segment(crate::pitr::SegmentAnchor {
                    segment_id: crate::pitr::SegmentId(3),
                    wal_digest: [5; 32],
                    seal_digest: [6; 32],
                }),
            },
            wal_digest: [7; 32],
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
