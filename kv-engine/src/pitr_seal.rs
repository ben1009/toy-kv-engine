//! Checksummed PITR v5 WAL seal sidecars.
//!
//! A seal records the exact logical WAL prefix and the ordered commit-time
//! index derived from its batches. The WAL remains independently recoverable;
//! the sidecar binds the complete identity of the WAL it describes - timeline,
//! archive epoch, segment, WAL format version, the full predecessor
//! [`ChainAnchor`], the logical length, and the SHA-256 digest of that prefix -
//! so a seal can never be replayed against a different segment.
//!
//! Layout (big-endian, no padding outside the declared fields):
//!
//! ```text
//!   0..8    magic "TKVSEAL1"
//!   8..10   version
//!  10..12   header length
//!  12..28   timeline id
//!  28..44   archive epoch id
//!  44..52   segment id
//!  52..54   WAL format version
//!  54..56   predecessor kind (0 genesis, 1 segment)
//!  56..64   predecessor segment id
//!  64..96   predecessor WAL digest
//!  96..128  predecessor seal digest
//! 128..160  WAL digest
//! 160..168  logical length
//! 168..176  batch count
//! 176..184  first commit timestamp
//! 184..192  last commit timestamp
//! 192..196  index crc32, over exactly the entry array
//! 196..200  header crc32, over bytes 0..196
//! ```
//!
//! followed by `batch_count` 20-byte entries of `(commit_ts, recorded_at_secs,
//! recorded_at_nanos)`. The file ends immediately after the final entry.
#![allow(dead_code)]

use anyhow::{Result, bail, ensure};
use sha2::{Digest, Sha256};

use crate::pitr::{ChainAnchor, RecordedAt, SegmentAnchor, WAL_V5_ALIGNMENT, WAL_V5_HEADER_LEN};

const MAGIC: &[u8; 8] = b"TKVSEAL1";
const VERSION: u16 = 1;
const PREDECESSOR_GENESIS: u16 = 0;
const PREDECESSOR_SEGMENT: u16 = 1;
const WAL_V5_FORMAT_VERSION: u16 = 5;
const HEADER_BYTES: usize =
    8 + 2 + 2 + 16 + 16 + 8 + 2 + 2 + 8 + 32 + 32 + 32 + 8 + 8 + 8 + 8 + 4 + 4;
const ENTRY_BYTES: usize = 8 + 8 + 4;
/// Offset the header checksum covers: everything declared before it.
const HEADER_CRC_COVERAGE: usize = HEADER_BYTES - 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SealEntry {
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
}

/// The identity fields a seal serializes. Deliberately narrower than
/// [`crate::pitr::WalV5Header`]: the predecessor is carried separately so a
/// decoded seal can never fabricate one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SealHeader {
    pub(crate) timeline_id: crate::pitr::TimelineId,
    pub(crate) archive_epoch_id: crate::pitr::ArchiveEpochId,
    pub(crate) segment_id: crate::pitr::SegmentId,
    pub(crate) wal_format_version: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct V5Seal {
    pub(crate) header: SealHeader,
    pub(crate) predecessor: ChainAnchor,
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
        output[52..54].copy_from_slice(&self.header.wal_format_version.to_be_bytes());
        let (
            predecessor_kind,
            predecessor_segment_id,
            predecessor_wal_digest,
            predecessor_seal_digest,
        ) = match self.predecessor {
            ChainAnchor::Genesis { .. } => (PREDECESSOR_GENESIS, 0, [0; 32], [0; 32]),
            ChainAnchor::Segment(SegmentAnchor {
                segment_id,
                wal_digest,
                seal_digest,
            }) => (PREDECESSOR_SEGMENT, segment_id.0, wal_digest, seal_digest),
        };
        output[54..56].copy_from_slice(&predecessor_kind.to_be_bytes());
        output[56..64].copy_from_slice(&predecessor_segment_id.to_be_bytes());
        output[64..96].copy_from_slice(&predecessor_wal_digest);
        output[96..128].copy_from_slice(&predecessor_seal_digest);
        output[128..160].copy_from_slice(&self.wal_digest);
        output[160..168].copy_from_slice(&self.logical_length.to_be_bytes());
        output[168..176].copy_from_slice(&count.to_be_bytes());
        let first = self.first_commit_ts().unwrap_or(0);
        let last = self.last_commit_ts().unwrap_or(0);
        output[176..184].copy_from_slice(&first.to_be_bytes());
        output[184..192].copy_from_slice(&last.to_be_bytes());
        let mut index_crc = crc32fast::Hasher::new();
        let mut offset = HEADER_BYTES;
        for entry in &self.entries {
            let mut record = [0u8; ENTRY_BYTES];
            record[..8].copy_from_slice(&entry.commit_ts.to_be_bytes());
            record[8..16].copy_from_slice(&entry.recorded_at.secs.to_be_bytes());
            record[16..20].copy_from_slice(&entry.recorded_at.nanos.to_be_bytes());
            output[offset..offset + ENTRY_BYTES].copy_from_slice(&record);
            index_crc.update(&record);
            offset += ENTRY_BYTES;
        }
        output[192..196].copy_from_slice(&index_crc.finalize().to_be_bytes());
        let header_crc = crc32fast::hash(&output[..HEADER_CRC_COVERAGE]);
        output[196..200].copy_from_slice(&header_crc.to_be_bytes());
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
            crc32fast::hash(&bytes[..HEADER_CRC_COVERAGE])
                == u32::from_be_bytes(bytes[HEADER_CRC_COVERAGE..HEADER_BYTES].try_into()?),
            "PITR seal header checksum mismatch"
        );
        let count = usize::try_from(u64::from_be_bytes(bytes[168..176].try_into()?))?;
        let body_len = count
            .checked_mul(ENTRY_BYTES)
            .ok_or_else(|| anyhow::anyhow!("PITR seal entry length overflow"))?;
        ensure!(
            bytes.len() == HEADER_BYTES + body_len,
            "PITR seal length mismatch"
        );
        let entries = decode_entries(&bytes[HEADER_BYTES..], count)?;
        let mut index_crc = crc32fast::Hasher::new();
        index_crc.update(&bytes[HEADER_BYTES..]);
        ensure!(
            index_crc.finalize() == u32::from_be_bytes(bytes[192..196].try_into()?),
            "PITR seal index checksum mismatch"
        );
        // The redundant range fields must agree with the index they summarize;
        // a checksum-valid header that disagrees with its own index is corrupt.
        let header_first = u64::from_be_bytes(bytes[176..184].try_into()?);
        let header_last = u64::from_be_bytes(bytes[184..192].try_into()?);
        let index_first = entries.first().map_or(0, |entry| entry.commit_ts);
        let index_last = entries.last().map_or(0, |entry| entry.commit_ts);
        ensure!(
            header_first == index_first && header_last == index_last,
            "PITR seal commit range disagrees with its index"
        );
        let predecessor_kind = u16::from_be_bytes(bytes[54..56].try_into()?);
        let predecessor = match predecessor_kind {
            PREDECESSOR_GENESIS => {
                let predecessor_segment_id = u64::from_be_bytes(bytes[56..64].try_into()?);
                ensure!(
                    predecessor_segment_id == 0
                        && bytes[64..96].iter().all(|byte| *byte == 0)
                        && bytes[96..128].iter().all(|byte| *byte == 0),
                    "PITR seal genesis predecessor carries segment identity"
                );
                ChainAnchor::Genesis {
                    archive_epoch_id: crate::pitr::ArchiveEpochId(bytes[28..44].try_into()?),
                }
            }
            PREDECESSOR_SEGMENT => ChainAnchor::Segment(SegmentAnchor {
                segment_id: crate::pitr::SegmentId(u64::from_be_bytes(bytes[56..64].try_into()?)),
                wal_digest: bytes[64..96].try_into()?,
                seal_digest: bytes[96..128].try_into()?,
            }),
            _ => bail!("unknown PITR seal predecessor kind"),
        };
        let seal = Self {
            header: SealHeader {
                timeline_id: crate::pitr::TimelineId(bytes[12..28].try_into()?),
                archive_epoch_id: crate::pitr::ArchiveEpochId(bytes[28..44].try_into()?),
                segment_id: crate::pitr::SegmentId(u64::from_be_bytes(bytes[44..52].try_into()?)),
                wal_format_version: u16::from_be_bytes(bytes[52..54].try_into()?),
            },
            predecessor,
            wal_digest: bytes[128..160].try_into()?,
            logical_length: u64::from_be_bytes(bytes[160..168].try_into()?),
            entries,
        };
        seal.validate()?;
        Ok(seal)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.header.wal_format_version == WAL_V5_FORMAT_VERSION,
            "unsupported PITR seal WAL format version"
        );
        ensure!(
            self.logical_length >= WAL_V5_HEADER_LEN as u64,
            "PITR seal logical length is below header"
        );
        ensure!(
            self.logical_length.is_multiple_of(WAL_V5_ALIGNMENT as u64),
            "PITR seal logical length is unaligned"
        );
        ensure!(self.wal_digest != [0; 32], "PITR seal WAL digest is empty");
        match &self.predecessor {
            ChainAnchor::Genesis { archive_epoch_id } => ensure!(
                *archive_epoch_id == self.header.archive_epoch_id,
                "PITR seal genesis predecessor binds a different epoch"
            ),
            ChainAnchor::Segment(SegmentAnchor {
                segment_id,
                wal_digest,
                seal_digest,
            }) => {
                ensure!(
                    wal_digest != &[0; 32],
                    "PITR seal predecessor WAL digest is empty"
                );
                ensure!(
                    seal_digest != &[0; 32],
                    "PITR seal predecessor seal digest is empty"
                );
                ensure!(
                    segment_id.0 < self.header.segment_id.0,
                    "PITR seal predecessor is not before the sealed segment"
                );
            }
        }
        let mut previous_ts = 0;
        let mut previous_recorded_at: Option<RecordedAt> = None;
        for entry in &self.entries {
            ensure!(entry.commit_ts != 0, "PITR seal commit timestamp is zero");
            ensure!(
                entry.recorded_at.nanos < 1_000_000_000,
                "PITR seal recorded time is invalid"
            );
            ensure!(
                previous_ts < entry.commit_ts,
                "PITR seal commit timestamps are not increasing"
            );
            if let Some(previous) = previous_recorded_at {
                ensure!(
                    previous <= entry.recorded_at,
                    "PITR seal recorded times are not nondecreasing"
                );
            }
            previous_ts = entry.commit_ts;
            previous_recorded_at = Some(entry.recorded_at);
        }
        // The redundant range fields must agree with the index they summarize,
        // otherwise a header that disagrees with its own index would validate.
        ensure!(
            (self.entries.is_empty() && self.logical_length == WAL_V5_HEADER_LEN as u64)
                || (!self.entries.is_empty() && self.logical_length > WAL_V5_HEADER_LEN as u64),
            "PITR seal empty/nonempty boundary is invalid"
        );
        Ok(())
    }
}

/// Decode `count` fixed-size entries, checking bounds before allocating.
fn decode_entries(bytes: &[u8], count: usize) -> Result<Vec<SealEntry>> {
    let mut entries = Vec::with_capacity(count);
    let mut offset = 0;
    for _ in 0..count {
        ensure!(
            bytes.len() >= offset + ENTRY_BYTES,
            "PITR seal index is truncated"
        );
        entries.push(SealEntry {
            commit_ts: u64::from_be_bytes(bytes[offset..offset + 8].try_into()?),
            recorded_at: RecordedAt {
                secs: i64::from_be_bytes(bytes[offset + 8..offset + 16].try_into()?),
                nanos: u32::from_be_bytes(bytes[offset + 16..offset + 20].try_into()?),
            },
        });
        offset += ENTRY_BYTES;
    }
    ensure!(offset == bytes.len(), "PITR seal index has trailing bytes");
    Ok(entries)
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
        header: SealHeader {
            timeline_id: header.timeline_id,
            archive_epoch_id: header.archive_epoch_id,
            segment_id: header.segment_id,
            wal_format_version: WAL_V5_FORMAT_VERSION,
        },
        predecessor: header.predecessor,
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

    fn header() -> SealHeader {
        SealHeader {
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(3),
            wal_format_version: WAL_V5_FORMAT_VERSION,
        }
    }

    fn predecessor() -> ChainAnchor {
        ChainAnchor::Segment(SegmentAnchor {
            segment_id: crate::pitr::SegmentId(2),
            wal_digest: [9; 32],
            seal_digest: [8; 32],
        })
    }

    #[test]
    fn empty_header_seal_round_trips() {
        let seal = V5Seal {
            header: header(),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
            wal_digest: [4; 32],
            logical_length: WAL_V5_HEADER_LEN as u64,
            entries: Vec::new(),
        };
        let encoded = seal.encode().unwrap();
        assert_eq!(V5Seal::decode(&encoded).unwrap(), seal);
    }

    #[test]
    fn segment_predecessor_survives_the_round_trip() {
        let seal = V5Seal {
            header: header(),
            predecessor: predecessor(),
            wal_digest: [4; 32],
            logical_length: WAL_V5_HEADER_LEN as u64 + 4096,
            entries: vec![SealEntry {
                commit_ts: 7,
                recorded_at: RecordedAt {
                    secs: 11,
                    nanos: 12,
                },
            }],
        };
        let encoded = seal.encode().unwrap();
        // The predecessor is what a genesis-only decode used to fabricate.
        assert_eq!(V5Seal::decode(&encoded).unwrap().predecessor, predecessor());
    }

    #[test]
    fn build_seal_indexes_v5_batches_and_logical_prefix() {
        let wal_header = crate::pitr::WalV5Header {
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(3),
            predecessor: predecessor(),
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
        let mut wal = crate::pitr::encode_v5_file_header(wal_header)
            .unwrap()
            .to_vec();
        wal.extend_from_slice(
            &crate::pitr::encode_v5_batch(&batch, crate::pitr::LIVE_WAL_V5_LIMITS).unwrap(),
        );
        let (seal, bytes) = build_v5_seal(&wal).unwrap();
        assert_eq!(seal.first_commit_ts(), Some(7));
        assert_eq!(seal.last_commit_ts(), Some(7));
        assert_eq!(seal.entries.len(), 1);
        assert_eq!(seal.predecessor, predecessor());
        assert_eq!(V5Seal::decode(&bytes).unwrap(), seal);
    }

    #[test]
    fn index_corruption_is_rejected() {
        let seal = V5Seal {
            header: header(),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
            wal_digest: [4; 32],
            logical_length: WAL_V5_HEADER_LEN as u64 + 4096,
            entries: vec![SealEntry {
                commit_ts: 7,
                recorded_at: RecordedAt {
                    secs: 11,
                    nanos: 12,
                },
            }],
        };
        let mut encoded = seal.encode().unwrap();
        // Flip a bit inside the entry array, past the header checksum.
        let last = encoded.len() - 1;
        encoded[last] ^= 0x01;
        assert!(V5Seal::decode(&encoded).is_err());
    }

    #[test]
    fn header_range_disagreeing_with_the_index_is_rejected() {
        let seal = V5Seal {
            header: header(),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
            wal_digest: [4; 32],
            logical_length: WAL_V5_HEADER_LEN as u64 + 4096,
            entries: vec![SealEntry {
                commit_ts: 7,
                recorded_at: RecordedAt {
                    secs: 11,
                    nanos: 12,
                },
            }],
        };
        let mut encoded = seal.encode().unwrap();
        // Rewrite `last_commit_ts` and repair the header checksum so only the
        // header/index disagreement remains.
        encoded[184..192].copy_from_slice(&99_u64.to_be_bytes());
        let header_crc = crc32fast::hash(&encoded[..HEADER_CRC_COVERAGE]);
        encoded[HEADER_CRC_COVERAGE..HEADER_BYTES].copy_from_slice(&header_crc.to_be_bytes());
        assert!(V5Seal::decode(&encoded).is_err());
    }
}
