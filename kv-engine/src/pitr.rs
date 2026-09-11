//! Crate-private contracts and codec primitives for PITR WAL v5.
//!
//! This module is deliberately dormant: the existing WAL writer and recovery
//! paths do not select v5 yet. The types here provide one canonical envelope
//! for the later sequencer, segment manager, and PITR replay implementation.
#![allow(dead_code)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};

pub(crate) const WAL_V5_MAGIC: [u8; 4] = *b"WAL2";
pub(crate) const WAL_V5_VERSION: u16 = 5;
pub(crate) const WAL_V5_HEADER_LEN: usize = 4096;
pub(crate) const WAL_V5_BATCH_HEADER_LEN: usize = 40;
pub(crate) const WAL_V5_ALIGNMENT: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct TimelineId(pub(crate) [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ArchiveEpochId(pub(crate) [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct SegmentId(pub(crate) u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentAnchor {
    pub(crate) segment_id: SegmentId,
    pub(crate) wal_digest: [u8; 32],
    pub(crate) seal_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChainAnchor {
    Genesis { archive_epoch_id: ArchiveEpochId },
    Segment(SegmentAnchor),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitTimeHighWater {
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) segment_id: SegmentId,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
    pub(crate) entry_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RecordedAt {
    pub(crate) secs: i64,
    pub(crate) nanos: u32,
}

impl RecordedAt {
    pub(crate) fn from_system_time(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => Self {
                secs: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
                nanos: duration.subsec_nanos(),
            },
            Err(error) => {
                let duration = error.duration();
                let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
                if duration.subsec_nanos() == 0 {
                    Self {
                        secs: -seconds,
                        nanos: 0,
                    }
                } else {
                    Self {
                        secs: -seconds - 1,
                        nanos: 1_000_000_000 - duration.subsec_nanos(),
                    }
                }
            }
        }
    }

    pub(crate) fn as_system_time(self) -> Result<SystemTime> {
        ensure!(self.nanos < 1_000_000_000, "recorded_at nanos out of range");
        if self.secs >= 0 {
            Ok(UNIX_EPOCH
                + Duration::from_secs(self.secs as u64)
                + Duration::from_nanos(u64::from(self.nanos)))
        } else {
            let seconds = self
                .secs
                .checked_neg()
                .context("recorded_at seconds overflow")?;
            let positive = Duration::from_secs(seconds as u64);
            let nanos = Duration::from_nanos(u64::from(self.nanos));
            positive
                .checked_sub(nanos)
                .and_then(|duration| UNIX_EPOCH.checked_sub(duration))
                .context("recorded_at before supported SystemTime range")
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    PointDelete { key: Vec<u8> },
    RangeDelete { start: Vec<u8>, end: Vec<u8> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalBatch {
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
    pub(crate) entries: Vec<WalEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV5Header {
    pub(crate) timeline_id: TimelineId,
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) segment_id: SegmentId,
    pub(crate) predecessor: ChainAnchor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedBatch {
    pub(crate) batch: WalBatch,
    pub(crate) logical_end: usize,
}

pub(crate) fn encode_v5_file_header(header: WalV5Header) -> [u8; WAL_V5_HEADER_LEN] {
    let mut output = [0; WAL_V5_HEADER_LEN];
    output[0..4].copy_from_slice(&WAL_V5_MAGIC);
    output[4..6].copy_from_slice(&WAL_V5_VERSION.to_be_bytes());
    output[8..10].copy_from_slice(&(WAL_V5_HEADER_LEN as u16).to_be_bytes());
    output[12..28].copy_from_slice(&header.timeline_id.0);
    output[28..44].copy_from_slice(&header.archive_epoch_id.0);
    output[44..52].copy_from_slice(&header.segment_id.0.to_be_bytes());
    match header.predecessor {
        ChainAnchor::Genesis { .. } => {}
        ChainAnchor::Segment(anchor) => {
            output[52] = 1;
            output[56..64].copy_from_slice(&anchor.segment_id.0.to_be_bytes());
            output[64..96].copy_from_slice(&anchor.wal_digest);
            output[96..128].copy_from_slice(&anchor.seal_digest);
        }
    }
    let header_crc = crc32fast::hash(&output[..128]);
    output[128..132].copy_from_slice(&header_crc.to_be_bytes());
    output
}

pub(crate) fn decode_v5_file_header(input: &[u8]) -> Result<WalV5Header> {
    ensure!(input.len() >= WAL_V5_HEADER_LEN, "truncated v5 WAL header");
    ensure!(input[0..4] == WAL_V5_MAGIC, "invalid v5 WAL magic");
    ensure!(
        u16::from_be_bytes([input[4], input[5]]) == WAL_V5_VERSION,
        "invalid v5 WAL version"
    );
    ensure!(
        u16::from_be_bytes([input[6], input[7]]) == 0,
        "unknown v5 WAL flags"
    );
    ensure!(
        u16::from_be_bytes([input[8], input[9]]) as usize == WAL_V5_HEADER_LEN,
        "invalid v5 header length"
    );
    ensure!(
        u16::from_be_bytes([input[10], input[11]]) == 0,
        "nonzero v5 reserved field"
    );
    ensure!(
        input[132..] == [0; WAL_V5_HEADER_LEN - 132],
        "nonzero v5 reserved bytes"
    );
    ensure!(
        crc32fast::hash(&input[..128]) == u32::from_be_bytes(input[128..132].try_into().unwrap()),
        "v5 header CRC mismatch"
    );
    let predecessor = match input[52] {
        0 => {
            ensure!(
                input[56..128] == [0; 72],
                "nonzero genesis predecessor fields"
            );
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId(input[28..44].try_into().unwrap()),
            }
        }
        1 => ChainAnchor::Segment(SegmentAnchor {
            segment_id: SegmentId(u64::from_be_bytes(input[56..64].try_into().unwrap())),
            wal_digest: input[64..96].try_into().unwrap(),
            seal_digest: input[96..128].try_into().unwrap(),
        }),
        _ => bail!("invalid v5 predecessor kind"),
    };
    ensure!(
        input[53..56] == [0; 3],
        "nonzero v5 predecessor reserved bytes"
    );
    Ok(WalV5Header {
        timeline_id: TimelineId(input[12..28].try_into().unwrap()),
        archive_epoch_id: ArchiveEpochId(input[28..44].try_into().unwrap()),
        segment_id: SegmentId(u64::from_be_bytes(input[44..52].try_into().unwrap())),
        predecessor,
    })
}

pub(crate) fn encode_v5_batch(batch: &WalBatch) -> Result<Vec<u8>> {
    ensure!(batch.commit_ts != 0, "v5 commit timestamp must be nonzero");
    ensure!(!batch.entries.is_empty(), "v5 batch must contain an entry");
    ensure!(
        batch.entries.len() <= u32::MAX as usize,
        "v5 entry count exceeds u32::MAX"
    );
    ensure!(
        batch.recorded_at.nanos < 1_000_000_000,
        "recorded_at nanos out of range"
    );
    let mut data = Vec::new();
    for entry in &batch.entries {
        let (kind, payload) = match entry {
            WalEntry::Put { key, value } => {
                let mut payload = Vec::new();
                put_len_prefixed(&mut payload, key)?;
                put_len_prefixed(&mut payload, value)?;
                (1, payload)
            }
            WalEntry::PointDelete { key } => {
                let mut payload = Vec::new();
                put_len_prefixed(&mut payload, key)?;
                (2, payload)
            }
            WalEntry::RangeDelete { start, end } => {
                ensure!(start < end, "invalid range tombstone ordering");
                let mut payload = Vec::new();
                put_len_prefixed(&mut payload, start)?;
                put_len_prefixed(&mut payload, end)?;
                (3, payload)
            }
        };
        data.push(kind);
        data.push(0);
        data.extend_from_slice(
            &(u32::try_from(payload.len()).context("v5 payload too large")?).to_be_bytes(),
        );
        data.extend_from_slice(&payload);
    }
    let data_len = u32::try_from(data.len()).context("v5 batch data too large")?;
    let mut output = vec![0; WAL_V5_BATCH_HEADER_LEN];
    output[0..8].copy_from_slice(&batch.commit_ts.to_be_bytes());
    output[8..16].copy_from_slice(&batch.recorded_at.secs.to_be_bytes());
    output[16..20].copy_from_slice(&batch.recorded_at.nanos.to_be_bytes());
    output[20..24].copy_from_slice(&(batch.entries.len() as u32).to_be_bytes());
    output[24..28].copy_from_slice(&data_len.to_be_bytes());
    output[28..32].copy_from_slice(&crc32fast::hash(&data).to_be_bytes());
    let header_crc = crc32fast::hash(&output[..28]);
    output[32..36].copy_from_slice(&header_crc.to_be_bytes());
    output.extend_from_slice(&data);
    let aligned_len = align_up(output.len())?;
    output.resize(aligned_len, 0);
    Ok(output)
}

pub(crate) fn decode_v5_batch(input: &[u8], offset: usize) -> Result<DecodedBatch> {
    ensure!(
        offset.is_multiple_of(WAL_V5_ALIGNMENT),
        "v5 batch offset is not aligned"
    );
    let header_end = offset
        .checked_add(WAL_V5_BATCH_HEADER_LEN)
        .context("v5 batch offset overflow")?;
    ensure!(input.len() >= header_end, "truncated v5 batch header");
    let header = &input[offset..header_end];
    let commit_ts = u64::from_be_bytes(header[0..8].try_into().unwrap());
    let recorded_at = RecordedAt {
        secs: i64::from_be_bytes(header[8..16].try_into().unwrap()),
        nanos: u32::from_be_bytes(header[16..20].try_into().unwrap()),
    };
    let entry_count = u32::from_be_bytes(header[20..24].try_into().unwrap()) as usize;
    let data_len = u32::from_be_bytes(header[24..28].try_into().unwrap()) as usize;
    ensure!(
        commit_ts != 0 && entry_count != 0 && data_len != 0,
        "invalid empty v5 batch"
    );
    ensure!(
        recorded_at.nanos < 1_000_000_000,
        "recorded_at nanos out of range"
    );
    ensure!(
        u32::from_be_bytes(header[32..36].try_into().unwrap()) == crc32fast::hash(&header[..28]),
        "v5 batch header CRC mismatch"
    );
    let data_start = header_end;
    let data_end = data_start
        .checked_add(data_len)
        .context("v5 batch data length overflow")?;
    let data = input
        .get(data_start..data_end)
        .context("truncated v5 batch data")?;
    ensure!(
        u32::from_be_bytes(header[28..32].try_into().unwrap()) == crc32fast::hash(data),
        "v5 batch data CRC mismatch"
    );
    ensure!(
        u32::from_be_bytes(header[36..40].try_into().unwrap()) == 0,
        "nonzero v5 batch reserved field"
    );
    let mut cursor: usize = 0;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let entry_header_end = cursor
            .checked_add(6)
            .context("v5 entry header length overflow")?;
        ensure!(entry_header_end <= data.len(), "truncated v5 entry header");
        let kind = data[cursor];
        ensure!(data[cursor + 1] == 0, "unknown v5 entry flags");
        let payload_len =
            u32::from_be_bytes(data[cursor + 2..cursor + 6].try_into().unwrap()) as usize;
        cursor += 6;
        let payload_end = cursor
            .checked_add(payload_len)
            .context("v5 entry payload length overflow")?;
        let payload = data
            .get(cursor..payload_end)
            .context("truncated v5 entry payload")?;
        entries.push(decode_entry(kind, payload)?);
        cursor += payload_len;
    }
    ensure!(cursor == data.len(), "v5 batch has trailing data");
    let logical_end = align_up(data_end)?;
    ensure!(input.len() >= logical_end, "truncated v5 alignment gap");
    ensure!(
        input[data_end..logical_end].iter().all(|byte| *byte == 0),
        "nonzero v5 alignment gap"
    );
    Ok(DecodedBatch {
        batch: WalBatch {
            commit_ts,
            recorded_at,
            entries,
        },
        logical_end,
    })
}

pub(crate) fn commit_time_entry_digest(high_water: CommitTimeHighWater) -> [u8; 32] {
    let mut preimage = Vec::with_capacity(16 + 8 + 8 + 8 + 4 + 20);
    preimage.extend_from_slice(b"PITR-COMMIT-TIME-V1");
    preimage.extend_from_slice(&high_water.archive_epoch_id.0);
    preimage.extend_from_slice(&high_water.segment_id.0.to_be_bytes());
    preimage.extend_from_slice(&high_water.commit_ts.to_be_bytes());
    preimage.extend_from_slice(&high_water.recorded_at.secs.to_be_bytes());
    preimage.extend_from_slice(&high_water.recorded_at.nanos.to_be_bytes());
    Sha256::digest(preimage).into()
}

fn decode_entry(kind: u8, payload: &[u8]) -> Result<WalEntry> {
    let mut cursor = 0;
    let first = read_len_prefixed(payload, &mut cursor)?;
    let entry = match kind {
        1 => WalEntry::Put {
            key: first,
            value: read_len_prefixed(payload, &mut cursor)?,
        },
        2 => WalEntry::PointDelete { key: first },
        3 => {
            let end = read_len_prefixed(payload, &mut cursor)?;
            ensure!(first < end, "invalid range tombstone ordering");
            WalEntry::RangeDelete { start: first, end }
        }
        _ => bail!("unknown v5 entry kind"),
    };
    ensure!(
        cursor == payload.len(),
        "v5 entry payload has trailing bytes"
    );
    Ok(entry)
}

fn put_len_prefixed(output: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    output.extend_from_slice(
        &u32::try_from(bytes.len())
            .context("v5 field too large")?
            .to_be_bytes(),
    );
    output.extend_from_slice(bytes);
    Ok(())
}

fn read_len_prefixed(input: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let length_end = cursor.checked_add(4).context("v5 length offset overflow")?;
    let length = u32::from_be_bytes(
        input
            .get(*cursor..length_end)
            .context("truncated v5 length")?
            .try_into()
            .unwrap(),
    ) as usize;
    *cursor = length_end;
    let value_end = cursor
        .checked_add(length)
        .context("v5 field length overflow")?;
    let bytes = input
        .get(*cursor..value_end)
        .context("truncated v5 field")?
        .to_vec();
    *cursor = value_end;
    Ok(bytes)
}

fn align_up(value: usize) -> Result<usize> {
    let rounded = value
        .checked_add(WAL_V5_ALIGNMENT - 1)
        .context("v5 alignment length overflow")?;
    Ok(rounded / WAL_V5_ALIGNMENT * WAL_V5_ALIGNMENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> WalV5Header {
        WalV5Header {
            timeline_id: TimelineId([1; 16]),
            archive_epoch_id: ArchiveEpochId([2; 16]),
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Segment(SegmentAnchor {
                segment_id: SegmentId(6),
                wal_digest: [3; 32],
                seal_digest: [4; 32],
            }),
        }
    }

    fn batch() -> WalBatch {
        WalBatch {
            commit_ts: 11,
            recorded_at: RecordedAt {
                secs: -2,
                nanos: 500_000_000,
            },
            entries: vec![
                WalEntry::Put {
                    key: b"a".to_vec(),
                    value: b"one".to_vec(),
                },
                WalEntry::PointDelete { key: b"b".to_vec() },
                WalEntry::RangeDelete {
                    start: b"c".to_vec(),
                    end: b"d".to_vec(),
                },
            ],
        }
    }

    #[test]
    fn recorded_at_uses_floor_representation_before_epoch() {
        let time = UNIX_EPOCH - Duration::from_millis(1500);
        assert_eq!(
            RecordedAt::from_system_time(time),
            RecordedAt {
                secs: -2,
                nanos: 500_000_000
            }
        );
        assert_eq!(
            RecordedAt::from_system_time(time).as_system_time().unwrap(),
            time
        );
    }

    #[test]
    fn v5_file_header_round_trips_and_has_zero_reserved_bytes() {
        let encoded = encode_v5_file_header(header());
        assert_eq!(&encoded[..4], b"WAL2");
        assert_eq!(u16::from_be_bytes([encoded[4], encoded[5]]), 5);
        assert!(encoded[132..].iter().all(|byte| *byte == 0));
        assert_eq!(decode_v5_file_header(&encoded).unwrap(), header());
    }

    #[test]
    fn v5_batch_round_trips_and_is_aligned() {
        let encoded = encode_v5_batch(&batch()).unwrap();
        assert_eq!(encoded.len() % WAL_V5_ALIGNMENT, 0);
        let decoded = decode_v5_batch(&encoded, 0).unwrap();
        assert_eq!(decoded.logical_end, encoded.len());
        assert_eq!(decoded.batch, batch());
    }

    #[test]
    fn v5_rejects_bad_crc_reserved_flags_and_trailing_data() {
        let encoded = encode_v5_batch(&batch()).unwrap();
        let mut bad_crc = encoded.clone();
        bad_crc[32] ^= 1;
        assert!(decode_v5_batch(&bad_crc, 0).is_err());

        let mut bad_reserved = encoded.clone();
        bad_reserved[39] = 1;
        assert!(decode_v5_batch(&bad_reserved, 0).is_err());

        let mut bad_flags = encoded;
        bad_flags[41] = 1;
        assert!(decode_v5_batch(&bad_flags, 0).is_err());
    }

    #[test]
    fn v5_commit_time_digest_binds_epoch_and_segment() {
        let high_water = CommitTimeHighWater {
            archive_epoch_id: ArchiveEpochId([2; 16]),
            segment_id: SegmentId(7),
            commit_ts: 11,
            recorded_at: batch().recorded_at,
            entry_digest: [0; 32],
        };
        let digest = commit_time_entry_digest(high_water);
        assert_ne!(digest, [0; 32]);
        let mut changed = high_water;
        changed.segment_id = SegmentId(8);
        assert_ne!(digest, commit_time_entry_digest(changed));
    }
}
