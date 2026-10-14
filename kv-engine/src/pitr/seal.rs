//! Checksummed PITR WAL seal sidecars.
//!
//! A v5-family seal records the exact logical WAL prefix and ordered commit-time
//! index. A v7 seal binds the immutable physical image and records each batch's
//! first DATA frame offset. The WAL remains independently recoverable; a sidecar
//! is accepted only when it binds to the supplied header and source image.
#![allow(dead_code)]

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use crate::{
    pitr::{
        ChainAnchor, RecordedAt, WAL_V5_ALIGNMENT, WAL_V5_HEADER_LEN, WalBatch, WalV5Header,
        WalV5Limits, decode_wal_entry_stream,
        manifest::{ActiveBoundary, ImmutableBoundary},
    },
    wal::v7::{
        codec::{
            WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WAL_V7_LOGICAL_BATCH_HEADER_LEN,
            WalV7DataFragmentHeader, WalV7FrameKind, WalV7Header, WalV7LogicalBatchHeader,
            decode_frame_structural,
        },
        recovery::{
            WalV7RecoveryAuthority, discover_frontier_candidates, select_recovery_candidate,
            select_recovery_candidate_with_limits,
        },
    },
};

#[cfg(test)]
use crate::wal::v7::codec::{
    WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY, WalV7Frontier, encode_data_frames_into,
    encode_frontier_successor, encode_generation_zero_frontier, encode_logical_batch,
    frame_record_digest,
};

const MAGIC: &[u8; 8] = b"TKVSEAL1";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 224;
const ENTRY_BYTES: usize = 24;
const V7_MAGIC: &[u8; 8] = b"TKVSEAL2";
const V7_VERSION: u16 = 2;
const V7_HEADER_BYTES: usize = 320;
const V7_ENTRY_BYTES: usize = 40;
// A v7 WAL needs one frame for its header, generation-zero frontier, and
// terminal frontier. Every ticket needs at least one DATA frame. This bounds
// both the ticket index and decoded sidecar size at the global WAL file limit.
const V7_MAX_ENTRY_COUNT: usize =
    (crate::wal::MAX_WAL_FILE_SIZE as usize - WAL_V7_HEADER_LEN - 2 * WAL_V7_FRAME_LEN)
        / WAL_V7_FRAME_LEN;
const V7_MAX_SEAL_BYTES: usize = V7_HEADER_BYTES + V7_MAX_ENTRY_COUNT * V7_ENTRY_BYTES;

fn v7_max_entry_count(wal_bytes: u64) -> Result<usize> {
    let fixed_bytes = u64::try_from(WAL_V7_HEADER_LEN + 2 * WAL_V7_FRAME_LEN)?;
    let data_bytes = wal_bytes.saturating_sub(fixed_bytes);
    usize::try_from(data_bytes / WAL_V7_FRAME_LEN as u64).map_err(Into::into)
}

fn v7_max_seal_bytes(entry_count: usize) -> Result<usize> {
    V7_HEADER_BYTES
        .checked_add(
            entry_count
                .checked_mul(V7_ENTRY_BYTES)
                .ok_or_else(|| anyhow::anyhow!("v7 PITR seal index length overflows"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("v7 PITR seal length overflows"))
}

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
        // The segment's own version, not the code's: a seal for a segment resumed
        // from before the logical-batch digest must keep naming the old rule, or
        // every later verification of that segment reads the wrong bytes.
        ensure!(
            crate::pitr::is_v5_family(self.header.wal_format_version),
            "unsupported PITR seal WAL format"
        );
        output[12..14].copy_from_slice(&self.header.wal_format_version.to_be_bytes());
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
            crate::pitr::is_v5_family(u16::from_be_bytes(bytes[12..14].try_into()?)),
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
            wal_format_version: u16::from_be_bytes(bytes[12..14].try_into()?),
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

/// One ticket's immutable v7 seal-index entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct V7SealEntry {
    pub(crate) ticket: u64,
    pub(crate) first_data_offset: u64,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
}

/// Seal metadata for one frozen v7 WAL physical image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct V7Seal {
    pub(crate) header: WalV7Header,
    pub(crate) boundary: ImmutableBoundary,
    pub(crate) entries: Vec<V7SealEntry>,
}

impl V7Seal {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let count = u64::try_from(self.entries.len())?;
        let body_len = count
            .checked_mul(V7_ENTRY_BYTES as u64)
            .ok_or_else(|| anyhow::anyhow!("v7 PITR seal index length overflows"))?;
        let total_len = V7_HEADER_BYTES
            .checked_add(usize::try_from(body_len)?)
            .ok_or_else(|| anyhow::anyhow!("v7 PITR seal length overflows"))?;
        let mut output = vec![0_u8; total_len];
        output[..8].copy_from_slice(V7_MAGIC);
        output[8..10].copy_from_slice(&V7_VERSION.to_be_bytes());
        output[10..12].copy_from_slice(&(V7_HEADER_BYTES as u16).to_be_bytes());
        output[12..14].copy_from_slice(&crate::wal::format::WAL_FORMAT_VERSION_V7.to_be_bytes());
        output[16..32].copy_from_slice(&self.header.timeline_id.0);
        output[32..48].copy_from_slice(&self.header.archive_epoch_id.0);
        output[48..56].copy_from_slice(&self.header.segment_id.0.to_be_bytes());
        match self.header.predecessor {
            ChainAnchor::Genesis { archive_epoch_id } => ensure!(
                archive_epoch_id == self.header.archive_epoch_id,
                "v7 seal genesis predecessor epoch mismatch"
            ),
            ChainAnchor::Segment(anchor) => {
                output[56] = 1;
                output[64..72].copy_from_slice(&anchor.segment_id.0.to_be_bytes());
                output[72..104].copy_from_slice(&anchor.wal_digest);
                output[104..136].copy_from_slice(&anchor.seal_digest);
            }
        }
        output[136..152].copy_from_slice(&self.header.incarnation);
        output[152..160].copy_from_slice(&self.boundary.active.ticket_end.to_be_bytes());
        output[160..168].copy_from_slice(&self.boundary.active.durable_end.to_be_bytes());
        output[168..176].copy_from_slice(&self.boundary.active.last_commit_ts.to_be_bytes());
        output[176..208].copy_from_slice(&self.boundary.active.prefix_digest);
        output[208..216].copy_from_slice(&self.boundary.sealed_end.to_be_bytes());
        output[216..248].copy_from_slice(&self.boundary.terminal_frontier_digest);
        output[248..280].copy_from_slice(&self.boundary.wal_digest);
        output[280..288].copy_from_slice(&count.to_be_bytes());
        output[288..296].copy_from_slice(
            &self
                .entries
                .first()
                .map_or(0, |entry| entry.commit_ts)
                .to_be_bytes(),
        );
        output[296..304].copy_from_slice(
            &self
                .entries
                .last()
                .map_or(0, |entry| entry.commit_ts)
                .to_be_bytes(),
        );
        output[304..312].copy_from_slice(&body_len.to_be_bytes());

        let mut offset = V7_HEADER_BYTES;
        for entry in &self.entries {
            output[offset..offset + 8].copy_from_slice(&entry.ticket.to_be_bytes());
            output[offset + 8..offset + 16].copy_from_slice(&entry.first_data_offset.to_be_bytes());
            output[offset + 16..offset + 24].copy_from_slice(&entry.commit_ts.to_be_bytes());
            output[offset + 24..offset + 32].copy_from_slice(&entry.recorded_at.secs.to_be_bytes());
            output[offset + 32..offset + 36]
                .copy_from_slice(&entry.recorded_at.nanos.to_be_bytes());
            offset += V7_ENTRY_BYTES;
        }
        let index_crc = crc32fast::hash(&output[V7_HEADER_BYTES..]);
        output[312..316].copy_from_slice(&index_crc.to_be_bytes());
        let header_crc = crc32fast::hash(&output[8..316]);
        output[316..320].copy_from_slice(&header_crc.to_be_bytes());

        Ok(output)
    }

    pub(crate) fn decode(bytes: &[u8], max_segment_bytes: u64) -> Result<Self> {
        ensure!(
            max_segment_bytes >= (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64,
            "persisted v7 PITR segment limit is too small"
        );
        ensure!(
            max_segment_bytes <= crate::wal::MAX_WAL_FILE_SIZE,
            "persisted v7 PITR segment limit exceeds the WAL format limit"
        );
        let max_entry_count = v7_max_entry_count(max_segment_bytes)?;
        let max_sidecar_bytes = v7_max_seal_bytes(max_entry_count)?;
        ensure!(
            bytes.len() <= max_sidecar_bytes && bytes.len() <= V7_MAX_SEAL_BYTES,
            "v7 PITR seal exceeds the persisted segment sidecar limit"
        );
        ensure!(bytes.len() >= V7_HEADER_BYTES, "v7 PITR seal is truncated");
        ensure!(&bytes[..8] == V7_MAGIC, "invalid v7 PITR seal magic");
        ensure!(
            u16::from_be_bytes(bytes[8..10].try_into()?) == V7_VERSION,
            "unsupported v7 PITR seal version"
        );
        ensure!(
            u16::from_be_bytes(bytes[10..12].try_into()?) as usize == V7_HEADER_BYTES,
            "invalid v7 PITR seal header length"
        );
        ensure!(
            u16::from_be_bytes(bytes[12..14].try_into()?)
                == crate::wal::format::WAL_FORMAT_VERSION_V7,
            "invalid v7 PITR seal WAL format"
        );
        ensure!(
            bytes[14..16].iter().all(|byte| *byte == 0)
                && bytes[57..64].iter().all(|byte| *byte == 0),
            "nonzero v7 PITR seal reserved field"
        );
        ensure!(
            crc32fast::hash(&bytes[8..316]) == u32::from_be_bytes(bytes[316..320].try_into()?),
            "v7 PITR seal header checksum mismatch"
        );

        let count = usize::try_from(u64::from_be_bytes(bytes[280..288].try_into()?))?;
        ensure!(
            count <= max_entry_count,
            "v7 PITR seal entry count exceeds the persisted segment limit"
        );
        let body_len = count
            .checked_mul(V7_ENTRY_BYTES)
            .ok_or_else(|| anyhow::anyhow!("v7 PITR seal index length overflows"))?;
        ensure!(
            u64::from_be_bytes(bytes[304..312].try_into()?) == u64::try_from(body_len)?,
            "v7 PITR seal index length field mismatch"
        );
        let total_len = V7_HEADER_BYTES
            .checked_add(body_len)
            .ok_or_else(|| anyhow::anyhow!("v7 PITR seal length overflows"))?;
        ensure!(bytes.len() == total_len, "v7 PITR seal length mismatch");
        ensure!(
            crc32fast::hash(&bytes[V7_HEADER_BYTES..])
                == u32::from_be_bytes(bytes[312..316].try_into()?),
            "v7 PITR seal index checksum mismatch"
        );

        let timeline_id = crate::pitr::TimelineId(bytes[16..32].try_into()?);
        let archive_epoch_id = crate::pitr::ArchiveEpochId(bytes[32..48].try_into()?);
        let segment_id = crate::pitr::SegmentId(u64::from_be_bytes(bytes[48..56].try_into()?));
        let predecessor = match bytes[56] {
            0 => {
                ensure!(
                    bytes[64..136].iter().all(|byte| *byte == 0),
                    "nonzero v7 seal genesis predecessor fields"
                );
                ChainAnchor::Genesis { archive_epoch_id }
            }
            1 => ChainAnchor::Segment(crate::pitr::SegmentAnchor {
                segment_id: crate::pitr::SegmentId(u64::from_be_bytes(bytes[64..72].try_into()?)),
                wal_digest: bytes[72..104].try_into()?,
                seal_digest: bytes[104..136].try_into()?,
            }),
            _ => anyhow::bail!("unknown v7 PITR seal predecessor kind"),
        };
        let header = WalV7Header {
            timeline_id,
            archive_epoch_id,
            segment_id,
            predecessor,
            incarnation: bytes[136..152].try_into()?,
        };
        let active = ActiveBoundary {
            timeline_id: timeline_id.0,
            archive_epoch_id: archive_epoch_id.0,
            segment_id: segment_id.0,
            incarnation: header.incarnation,
            ticket_end: u64::from_be_bytes(bytes[152..160].try_into()?),
            durable_end: u64::from_be_bytes(bytes[160..168].try_into()?),
            last_commit_ts: u64::from_be_bytes(bytes[168..176].try_into()?),
            prefix_digest: bytes[176..208].try_into()?,
        };
        let boundary = ImmutableBoundary {
            active,
            sealed_end: u64::from_be_bytes(bytes[208..216].try_into()?),
            terminal_frontier_digest: bytes[216..248].try_into()?,
            wal_digest: bytes[248..280].try_into()?,
        };
        ensure!(
            boundary.sealed_end <= max_segment_bytes,
            "v7 PITR sealed image exceeds the persisted segment limit"
        );
        ensure!(
            count <= v7_max_entry_count(boundary.sealed_end)?,
            "v7 PITR seal entry count exceeds its sealed image capacity"
        );
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|error| anyhow::anyhow!("reserve v7 PITR seal entries: {error}"))?;
        let mut offset = V7_HEADER_BYTES;
        for _ in 0..count {
            entries.push(V7SealEntry {
                ticket: u64::from_be_bytes(bytes[offset..offset + 8].try_into()?),
                first_data_offset: u64::from_be_bytes(bytes[offset + 8..offset + 16].try_into()?),
                commit_ts: u64::from_be_bytes(bytes[offset + 16..offset + 24].try_into()?),
                recorded_at: RecordedAt {
                    secs: i64::from_be_bytes(bytes[offset + 24..offset + 32].try_into()?),
                    nanos: u32::from_be_bytes(bytes[offset + 32..offset + 36].try_into()?),
                },
            });
            ensure!(
                bytes[offset + 36..offset + 40]
                    .iter()
                    .all(|byte| *byte == 0),
                "nonzero v7 PITR seal index reserved field"
            );
            offset += V7_ENTRY_BYTES;
        }
        ensure!(
            u64::from_be_bytes(bytes[288..296].try_into()?)
                == entries.first().map_or(0, |entry| entry.commit_ts)
                && u64::from_be_bytes(bytes[296..304].try_into()?)
                    == entries.last().map_or(0, |entry| entry.commit_ts),
            "v7 PITR seal commit range mismatch"
        );
        let seal = Self {
            header,
            boundary,
            entries,
        };
        seal.validate()?;

        Ok(seal)
    }

    /// Verify the sidecar against a frozen v7 WAL image, including the terminal
    /// FRONTIER, complete physical digest, and every ticket/index entry.
    pub(crate) fn verify_wal(&self, wal: &[u8]) -> Result<()> {
        self.verify_wal_with_batch_visitor(wal, crate::pitr::LIVE_WAL_V5_LIMITS, |_| Ok(()))
    }

    /// Verify the frozen image and decode its ticket-ordered replay batches.
    pub(crate) fn decode_wal_batches(
        &self,
        wal: &[u8],
        limits: WalV5Limits,
    ) -> Result<Vec<WalBatch>> {
        let mut batches = Vec::new();
        self.verify_wal_with_batch_visitor(wal, limits, |batch| {
            batches
                .try_reserve(1)
                .map_err(|error| anyhow::anyhow!("reserve v7 PITR restore batches: {error}"))?;
            batches.push(batch);
            Ok(())
        })?;
        Ok(batches)
    }

    fn verify_wal_with_batch_visitor(
        &self,
        wal: &[u8],
        limits: WalV5Limits,
        mut visit_batch: impl FnMut(WalBatch) -> Result<()>,
    ) -> Result<()> {
        self.validate()?;
        ensure!(
            u64::try_from(wal.len())? == self.boundary.sealed_end,
            "v7 PITR WAL length differs from its immutable seal"
        );
        let header_bytes = wal
            .get(..WAL_V7_HEADER_LEN)
            .context("v7 PITR WAL is shorter than its header")?;
        let header = WalV7Header::decode(header_bytes)?;
        ensure!(
            header.encode()? == header_bytes && header == self.header,
            "v7 PITR seal identity differs from the WAL header"
        );
        let mut reader = std::io::Cursor::new(wal);
        let header_digest = header.digest()?;
        let discovery =
            discover_frontier_candidates(&mut reader, u64::try_from(wal.len())?, &header_digest)?;
        let selection = select_recovery_candidate_with_limits(
            &mut reader,
            u64::try_from(wal.len())?,
            &discovery,
            WalV7RecoveryAuthority::Sealed {
                predecessor: header.predecessor,
                boundary: self.boundary,
            },
            limits,
        )?;
        ensure!(
            selection.candidate.frame_digest == self.boundary.terminal_frontier_digest,
            "v7 PITR WAL terminal FRONTIER differs from its seal"
        );
        let mut entry_index = 0_usize;
        walk_v7_seal_entries(wal, header, limits, |entry, batch| {
            ensure!(
                self.entries.get(entry_index) == Some(&entry),
                "v7 PITR WAL batches or seal index differ from the sidecar"
            );
            entry_index = entry_index
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("v7 PITR seal entry index overflows"))?;
            visit_batch(batch)
        })?;
        ensure!(
            entry_index == self.entries.len(),
            "v7 PITR WAL batches or seal index differ from the sidecar"
        );

        Ok(())
    }

    fn validate(&self) -> Result<()> {
        self.boundary.validate()?;
        let active = self.boundary.active;
        ensure!(
            active.timeline_id == self.header.timeline_id.0
                && active.archive_epoch_id == self.header.archive_epoch_id.0
                && active.segment_id == self.header.segment_id.0
                && active.incarnation == self.header.incarnation,
            "v7 PITR seal identity differs from its immutable boundary"
        );
        ensure!(
            self.header.incarnation != [0; 16],
            "v7 PITR seal incarnation is empty"
        );
        let expected_sealed_end = active
            .durable_end
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("v7 PITR seal length overflows"))?;
        ensure!(
            self.boundary.sealed_end == expected_sealed_end,
            "v7 PITR seal does not end immediately after its terminal FRONTIER"
        );
        ensure!(
            self.boundary.sealed_end <= crate::wal::MAX_WAL_FILE_SIZE,
            "v7 PITR sealed image exceeds the WAL format limit"
        );
        ensure!(
            self.boundary.wal_digest != [0; 32]
                && self.boundary.terminal_frontier_digest != [0; 32]
                && active.prefix_digest != [0; 32],
            "v7 PITR seal digest is empty"
        );
        ensure!(
            u64::try_from(self.entries.len())? == active.ticket_end,
            "v7 PITR seal index count differs from its terminal ticket"
        );
        ensure!(
            self.entries.len() <= V7_MAX_ENTRY_COUNT,
            "v7 PITR seal index exceeds the WAL format limit"
        );
        ensure!(
            self.entries.len() <= v7_max_entry_count(self.boundary.sealed_end)?,
            "v7 PITR seal index exceeds its sealed image capacity"
        );
        ensure!(
            active.last_commit_ts == self.entries.last().map_or(0, |entry| entry.commit_ts),
            "v7 PITR seal final timestamp differs from its index"
        );
        if active.ticket_end == 0 {
            ensure!(
                self.boundary.sealed_end == 2 * WAL_V7_FRAME_LEN as u64,
                "empty v7 PITR seal has an invalid image length"
            );
            ensure!(
                active.prefix_digest == self.header.digest()?,
                "empty v7 PITR seal prefix digest differs from its header"
            );
        }

        let terminal_offset = self
            .boundary
            .sealed_end
            .checked_sub(WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("v7 PITR terminal offset underflows"))?;
        let mut previous_offset = None;
        let mut previous_commit_ts = 0;
        let mut previous_recorded_at = None;
        for (index, entry) in self.entries.iter().enumerate() {
            ensure!(
                entry.ticket == u64::try_from(index)?,
                "v7 PITR seal tickets are not contiguous"
            );
            ensure!(
                entry.first_data_offset >= (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64
                    && entry
                        .first_data_offset
                        .is_multiple_of(WAL_V7_FRAME_LEN as u64)
                    && entry.first_data_offset < terminal_offset,
                "v7 PITR seal DATA index offset is invalid"
            );
            ensure!(
                previous_offset.is_none_or(|previous| previous < entry.first_data_offset),
                "v7 PITR seal DATA index offsets are not increasing"
            );
            ensure!(
                entry.commit_ts > previous_commit_ts,
                "v7 PITR seal commit timestamps are not increasing"
            );
            ensure!(
                entry.recorded_at.nanos < 1_000_000_000,
                "v7 PITR seal recorded time is invalid"
            );
            ensure!(
                previous_recorded_at.is_none_or(|previous| entry.recorded_at >= previous),
                "v7 PITR seal recorded times are decreasing"
            );
            previous_offset = Some(entry.first_data_offset);
            previous_commit_ts = entry.commit_ts;
            previous_recorded_at = Some(entry.recorded_at);
        }
        match self.header.predecessor {
            ChainAnchor::Genesis { archive_epoch_id } => ensure!(
                archive_epoch_id == self.header.archive_epoch_id,
                "v7 PITR seal genesis predecessor epoch mismatch"
            ),
            ChainAnchor::Segment(anchor) => ensure!(
                anchor.wal_digest != [0; 32] && anchor.seal_digest != [0; 32],
                "v7 PITR seal predecessor digest is empty"
            ),
        }
        Ok(())
    }
}

/// Build the v7 seal only from a complete image whose final frame is the
/// authority-bound terminal FRONTIER. Recovery revalidates every frame and the
/// logical anchor before the sidecar is encoded.
pub(crate) fn build_v7_seal(
    wal: &[u8],
    active: ActiveBoundary,
    predecessor: ChainAnchor,
) -> Result<(V7Seal, Vec<u8>)> {
    ensure!(
        wal.len() >= WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN,
        "v7 PITR WAL is truncated"
    );
    ensure!(
        wal.len().is_multiple_of(WAL_V7_FRAME_LEN),
        "v7 PITR WAL length is unaligned"
    );
    let sealed_end = u64::try_from(wal.len())?;
    ensure!(
        sealed_end <= crate::wal::MAX_WAL_FILE_SIZE,
        "v7 PITR sealed image exceeds the WAL format limit"
    );
    let terminal_offset = sealed_end
        .checked_sub(WAL_V7_FRAME_LEN as u64)
        .ok_or_else(|| anyhow::anyhow!("v7 PITR terminal offset underflows"))?;
    let header_bytes = &wal[..WAL_V7_HEADER_LEN];
    let header = WalV7Header::decode(header_bytes)?;
    ensure!(
        header.encode()? == header_bytes,
        "v7 PITR WAL header is not canonical"
    );
    ensure!(
        header.predecessor == predecessor,
        "v7 PITR WAL predecessor differs from the manifest"
    );
    let header_digest = header.digest()?;
    let wal_digest: [u8; 32] = Sha256::digest(wal).into();
    let mut reader = std::io::Cursor::new(wal);
    let discovery = discover_frontier_candidates(&mut reader, sealed_end, &header_digest)?;
    let terminal = discovery
        .candidates
        .iter()
        .find(|candidate| candidate.frame_offset == terminal_offset)
        .context("v7 PITR WAL has no canonical terminal FRONTIER")?;
    let boundary = ImmutableBoundary {
        active,
        sealed_end,
        terminal_frontier_digest: terminal.frame_digest,
        wal_digest,
    };
    let selection = select_recovery_candidate(
        &mut reader,
        sealed_end,
        &discovery,
        WalV7RecoveryAuthority::Sealed {
            predecessor,
            boundary,
        },
    )?;
    ensure!(
        selection.candidate.frame_offset == terminal_offset,
        "v7 PITR terminal FRONTIER is not at the end of the sealed image"
    );

    let seal = V7Seal {
        header,
        boundary,
        entries: collect_v7_seal_entries(wal, header)?,
    };
    seal.validate()?;
    let bytes = seal.encode()?;
    ensure!(
        V7Seal::decode(&bytes, sealed_end)? == seal,
        "v7 PITR seal codec round-trip mismatch"
    );

    Ok((seal, bytes))
}

fn collect_v7_seal_entries(wal: &[u8], header: WalV7Header) -> Result<Vec<V7SealEntry>> {
    let mut entries = Vec::new();
    walk_v7_seal_entries(wal, header, crate::pitr::LIVE_WAL_V5_LIMITS, |entry, _| {
        entries
            .try_reserve(1)
            .map_err(|error| anyhow::anyhow!("grow v7 PITR seal index: {error}"))?;
        entries.push(entry);
        Ok(())
    })?;
    Ok(entries)
}

fn walk_v7_seal_entries(
    wal: &[u8],
    header: WalV7Header,
    limits: WalV5Limits,
    mut visit_batch: impl FnMut(V7SealEntry, WalBatch) -> Result<()>,
) -> Result<()> {
    let limits = limits.validate()?;
    let max_batch_len = WAL_V7_LOGICAL_BATCH_HEADER_LEN
        .checked_add(limits.max_batch_data_bytes)
        .ok_or_else(|| anyhow::anyhow!("v7 PITR batch byte limit overflows"))?;
    let sealed_end = u64::try_from(wal.len())?;
    ensure!(
        sealed_end >= WAL_V7_HEADER_LEN as u64 + WAL_V7_FRAME_LEN as u64
            && sealed_end.is_multiple_of(WAL_V7_FRAME_LEN as u64),
        "v7 PITR WAL image has an invalid length"
    );
    let terminal_offset = sealed_end - WAL_V7_FRAME_LEN as u64;
    let header_digest = header.digest()?;
    let data_start = WAL_V7_HEADER_LEN as u64 + WAL_V7_FRAME_LEN as u64;
    if terminal_offset < data_start {
        ensure!(
            terminal_offset == WAL_V7_HEADER_LEN as u64,
            "empty v7 PITR WAL has an invalid terminal FRONTIER offset"
        );
        return Ok(());
    }
    let mut offset = data_start;
    let mut expected_ticket = 0_u64;
    while offset < terminal_offset {
        let frame_start = usize::try_from(offset)?;
        let frame_end = frame_start
            .checked_add(WAL_V7_FRAME_LEN)
            .ok_or_else(|| anyhow::anyhow!("v7 PITR frame range overflows"))?;
        let encoded = wal
            .get(frame_start..frame_end)
            .context("v7 PITR frame is truncated before terminal FRONTIER")?;
        let frame = decode_frame_structural(encoded, offset, &header_digest)?;
        match frame.kind {
            WalV7FrameKind::Frontier => {
                let frontier = crate::wal::v7::codec::WalV7Frontier::decode_body(&frame.body)?;
                if frontier.generation != 0 {
                    ensure!(
                        frontier.ticket_end <= expected_ticket,
                        "v7 PITR FRONTIER covers a batch absent from its physical prefix"
                    );
                }
                offset += WAL_V7_FRAME_LEN as u64;
            }
            WalV7FrameKind::Data => {
                let (first, payload) = WalV7DataFragmentHeader::decode_body(&frame.body)?;
                ensure!(
                    first.segment_ticket == expected_ticket && first.fragment_index == 0,
                    "v7 PITR DATA tickets are not contiguous"
                );
                let batch_len = usize::try_from(first.batch_bytes)?;
                ensure!(
                    batch_len <= max_batch_len,
                    "v7 PITR DATA batch exceeds configured byte limit"
                );
                let mut logical = Vec::new();
                logical
                    .try_reserve_exact(batch_len)
                    .map_err(|error| anyhow::anyhow!("reserve v7 PITR batch: {error}"))?;
                logical.extend_from_slice(payload);
                for fragment_index in 1..usize::try_from(first.fragment_count)? {
                    let fragment_offset = offset
                        .checked_add(
                            u64::try_from(fragment_index)?
                                .checked_mul(WAL_V7_FRAME_LEN as u64)
                                .ok_or_else(|| anyhow::anyhow!("v7 fragment offset overflows"))?,
                        )
                        .ok_or_else(|| anyhow::anyhow!("v7 fragment offset overflows"))?;
                    ensure!(
                        fragment_offset < terminal_offset,
                        "v7 PITR DATA batch extends into terminal FRONTIER"
                    );
                    let fragment_start = usize::try_from(fragment_offset)?;
                    let fragment_end = fragment_start
                        .checked_add(WAL_V7_FRAME_LEN)
                        .ok_or_else(|| anyhow::anyhow!("v7 fragment range overflows"))?;
                    let fragment_encoded = wal
                        .get(fragment_start..fragment_end)
                        .context("v7 PITR DATA fragment is truncated")?;
                    let fragment_frame =
                        decode_frame_structural(fragment_encoded, fragment_offset, &header_digest)?;
                    ensure!(
                        fragment_frame.kind == WalV7FrameKind::Data,
                        "v7 PITR DATA batch is interleaved"
                    );
                    let (fragment, fragment_payload) =
                        WalV7DataFragmentHeader::decode_body(&fragment_frame.body)?;
                    ensure!(
                        fragment.segment_ticket == first.segment_ticket
                            && fragment.fragment_index == u32::try_from(fragment_index)?
                            && fragment.fragment_count == first.fragment_count
                            && fragment.batch_bytes == first.batch_bytes,
                        "v7 PITR DATA fragments disagree"
                    );
                    logical.extend_from_slice(fragment_payload);
                }
                ensure!(
                    logical.len() == batch_len,
                    "v7 PITR DATA batch length mismatch"
                );
                ensure!(
                    batch_len >= WAL_V7_LOGICAL_BATCH_HEADER_LEN,
                    "v7 PITR DATA batch is shorter than its logical header"
                );
                let data = &logical[WAL_V7_LOGICAL_BATCH_HEADER_LEN..];
                let logical_header = WalV7LogicalBatchHeader::decode(
                    &logical[..WAL_V7_LOGICAL_BATCH_HEADER_LEN],
                    data,
                    first.segment_ticket,
                    first.batch_bytes,
                    limits,
                )?;
                let recorded_at = RecordedAt {
                    secs: logical_header.recorded_at_secs,
                    nanos: logical_header.recorded_at_nanos,
                };
                let batch = decode_wal_entry_stream(
                    logical_header.commit_ts,
                    recorded_at,
                    logical_header.entry_count,
                    data,
                    limits,
                )?;
                visit_batch(
                    V7SealEntry {
                        ticket: expected_ticket,
                        first_data_offset: offset,
                        commit_ts: batch.commit_ts,
                        recorded_at: batch.recorded_at,
                    },
                    batch,
                )?;
                expected_ticket = expected_ticket
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("v7 PITR ticket count overflows"))?;
                offset = offset
                    .checked_add(
                        u64::from(first.fragment_count)
                            .checked_mul(WAL_V7_FRAME_LEN as u64)
                            .ok_or_else(|| anyhow::anyhow!("v7 DATA batch span overflows"))?,
                    )
                    .ok_or_else(|| anyhow::anyhow!("v7 DATA batch end overflows"))?;
            }
        }
    }
    ensure!(
        offset == terminal_offset,
        "v7 PITR WAL has bytes between batches and terminal FRONTIER"
    );
    Ok(())
}

/// Walk a v5-family segment: collect its seal entries, extend `hasher` with
/// exactly the bytes `rule` covers, and return the aligned logical length the walk
/// ended at. One implementation serves both rules, so a rebuild and a live
/// accumulator can never disagree about what a digest covers.
pub(crate) fn walk_v5_segment(
    hasher: &mut Sha256,
    wal: &[u8],
    rule: crate::pitr::WalDigestRule,
) -> Result<(u64, Vec<SealEntry>)> {
    ensure!(wal.len() >= WAL_V5_HEADER_LEN, "PITR WAL is truncated");
    let mut offset = WAL_V5_HEADER_LEN;
    let mut entries = Vec::new();
    let mut spans = Vec::new();

    while offset < wal.len() {
        if wal[offset..].iter().all(|byte| *byte == 0) {
            break;
        }
        let decoded = crate::pitr::decode_v5_batch(wal, offset, crate::pitr::LIVE_WAL_V5_LIMITS)?;
        entries.push(SealEntry {
            commit_ts: decoded.batch.commit_ts,
            recorded_at: decoded.batch.recorded_at,
        });
        spans.push((offset, decoded.logical_end, decoded.data_end - offset));
        offset = decoded.logical_end;
    }
    match rule {
        // A prefix rule covers every byte written so far, including the padding
        // between and after the batches.
        crate::pitr::WalDigestRule::WholeAlignedPrefix => hasher.update(&wal[..offset]),
        crate::pitr::WalDigestRule::LogicalBatches => {
            hasher.update(&wal[..WAL_V5_HEADER_LEN]);
            for (start, end, logical_len) in spans {
                hasher.update(rule.batch_slice(&wal[start..end], logical_len));
            }
        }
    }
    Ok((offset as u64, entries))
}

/// Recompute a segment's `wal_digest` from its bytes.
///
/// `rule` is checked against the rule the file's own version selects rather than
/// trusted: every caller passes a rule it took from somewhere else - a catalog
/// entry, a seal echo, a caller's parameter - and a mismatch there would otherwise
/// show up only as a digest that never matches.
pub(crate) fn wal_digest(wal: &[u8], rule: crate::pitr::WalDigestRule) -> Result<[u8; 32]> {
    let file_rule =
        crate::pitr::wal_digest_rule(crate::pitr::decode_v5_file_header(wal)?.wal_format_version)?;
    ensure!(
        file_rule == rule,
        "PITR WAL digest rule does not match the segment's own format version"
    );
    let mut hasher = Sha256::new();
    walk_v5_segment(&mut hasher, wal, rule)?;

    Ok(hasher.finalize().into())
}

pub(crate) fn build_v5_seal(wal: &[u8]) -> Result<(V5Seal, Vec<u8>)> {
    let header = crate::pitr::decode_v5_file_header(wal)?;
    // The file decides the rule, so a segment written before the change keeps
    // verifying under the rule it was written with.
    let rule = crate::pitr::wal_digest_rule(header.wal_format_version)?;
    let mut hasher = Sha256::new();
    let (logical_length, entries) = walk_v5_segment(&mut hasher, wal, rule)?;
    let wal_digest = hasher.finalize().into();
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
    use std::io::{Cursor, Read, Seek, SeekFrom};

    use super::*;

    struct PrefixReader<'a> {
        cursor: Cursor<&'a [u8]>,
        read_limit: u64,
        read_beyond_limit: bool,
    }

    impl Read for PrefixReader<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.cursor.position() + buffer.len() as u64 > self.read_limit {
                self.read_beyond_limit = true;
                return Err(std::io::Error::other(
                    "read beyond first oversized DATA fragment",
                ));
            }
            self.cursor.read(buffer)
        }
    }

    impl Seek for PrefixReader<'_> {
        fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
            self.cursor.seek(position)
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(
            concat!(env!("CARGO_MANIFEST_DIR"), "/src/tests/fixtures/",).to_string() + name,
        )
        .unwrap()
    }

    fn v7_fixture(batches: &[(u64, RecordedAt)]) -> Result<(Vec<u8>, WalV7Header, ActiveBoundary)> {
        let epoch = crate::pitr::ArchiveEpochId([2; 16]);
        let header = WalV7Header {
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: epoch,
            segment_id: crate::pitr::SegmentId(3),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: epoch,
            },
            incarnation: [4; 16],
        };
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
        let generation_zero = encode_generation_zero_frontier(header_digest)?;
        let mut previous_frontier = WalV7Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let mut previous_frontier_offset = WAL_V7_HEADER_LEN as u64;
        let mut previous_frontier_digest = frame_record_digest(&generation_zero)?;
        let mut image = header_bytes.to_vec();
        image.extend_from_slice(&generation_zero);
        let mut logical_hasher = Sha256::new();
        logical_hasher.update(header_bytes);
        let limits = crate::pitr::LIVE_WAL_V5_LIMITS;

        for (ticket, &(commit_ts, recorded_at)) in batches.iter().enumerate() {
            let ticket = u64::try_from(ticket)?;
            let batch = crate::pitr::WalBatch {
                commit_ts,
                recorded_at,
                entries: vec![crate::pitr::WalEntry::Put {
                    key: format!("key-{ticket}").into_bytes(),
                    value: format!("value-{ticket}").into_bytes(),
                }],
            };
            let logical = encode_logical_batch(ticket, &batch, limits)?;
            let fragment_count =
                crate::wal::v7::codec::data_fragment_count(u64::try_from(logical.len())?)
                    .context("v7 test batch has no valid fragment count")?;
            let frame_bytes = usize::try_from(fragment_count)?
                .checked_mul(WAL_V7_FRAME_LEN)
                .context("v7 test frame length overflows")?;
            let first_data_offset = u64::try_from(image.len())?;
            let mut frames = vec![0; frame_bytes];
            let written = encode_data_frames_into(
                header_digest,
                first_data_offset,
                ticket,
                &logical,
                limits,
                &mut frames,
            )?;
            ensure!(
                written == frame_bytes,
                "v7 test batch frame length mismatch"
            );
            image.extend_from_slice(&frames);
            logical_hasher.update(&logical);

            let frame_offset = u64::try_from(image.len())?;
            let frontier = WalV7Frontier {
                generation: ticket + 1,
                ticket_end: ticket + 1,
                durable_end: frame_offset,
                last_commit_ts: commit_ts,
                prefix_digest: logical_hasher.clone().finalize().into(),
                previous_frontier_offset,
                previous_frontier_digest,
            };
            let frame = encode_frontier_successor(
                frontier,
                previous_frontier,
                previous_frontier_offset,
                &previous_frontier_digest,
                frame_offset,
                header_digest,
            )?;
            previous_frontier_digest = frame_record_digest(&frame)?;
            previous_frontier_offset = frame_offset;
            previous_frontier = frontier;
            image.extend_from_slice(&frame);
        }

        let active = ActiveBoundary {
            timeline_id: header.timeline_id.0,
            archive_epoch_id: header.archive_epoch_id.0,
            segment_id: header.segment_id.0,
            incarnation: header.incarnation,
            ticket_end: u64::try_from(batches.len())?,
            durable_end: if batches.is_empty() {
                WAL_V7_HEADER_LEN as u64
            } else {
                previous_frontier_offset
            },
            last_commit_ts: batches.last().map_or(0, |(commit_ts, _)| *commit_ts),
            prefix_digest: logical_hasher.finalize().into(),
        };
        Ok((image, header, active))
    }

    fn refresh_v7_seal_header_crc(bytes: &mut [u8]) {
        let header_crc = crc32fast::hash(&bytes[8..316]);
        bytes[316..320].copy_from_slice(&header_crc.to_be_bytes());
    }

    /// A v6 digest against a preimage written out by hand.
    ///
    /// Without this the suite cannot tell a narrowed digest from a padded one: every
    /// other v6 expectation is either computed by the code under test or satisfied by
    /// a different mechanism. The padding-tamper assertions pass under either rule
    /// (the parser refuses a nonzero gap byte before any digest is compared), and the
    /// "digest differs from the whole file" assertion holds for a file with a
    /// preallocated tail whatever the rule. Here the bytes are enumerated explicitly -
    /// header, then each batch's own `data_end - offset` bytes - so a digest that
    /// stopped stripping the padding disagrees and fails.
    #[test]
    fn v6_digest_covers_the_header_and_each_batch_without_its_padding() {
        let header = crate::pitr::WalV5Header {
            wal_format_version: crate::pitr::WAL_V5_VERSION,
            timeline_id: crate::pitr::TimelineId([7; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([8; 16]),
            segment_id: crate::pitr::SegmentId(9),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([8; 16]),
            },
        };
        let mut wal = crate::pitr::encode_v5_file_header(header).unwrap().to_vec();
        for (commit_ts, key, value) in [
            (1_u64, &b"alpha"[..], &b"one"[..]),
            (2, &b"beta"[..], &b"two-longer-value"[..]),
        ] {
            wal.extend_from_slice(
                &crate::pitr::encode_v5_batch(
                    &crate::pitr::WalBatch {
                        commit_ts,
                        recorded_at: RecordedAt {
                            secs: commit_ts as i64,
                            nanos: 7,
                        },
                        entries: vec![crate::pitr::WalEntry::Put {
                            key: key.to_vec(),
                            value: value.to_vec(),
                        }],
                    },
                    crate::pitr::LIVE_WAL_V5_LIMITS,
                )
                .unwrap(),
            );
        }

        let mut expected = Sha256::new();
        expected.update(&wal[..WAL_V5_HEADER_LEN]);
        let mut offset = WAL_V5_HEADER_LEN;
        let mut padding = 0_usize;
        let mut batches = 0_usize;
        while offset < wal.len() {
            let decoded =
                crate::pitr::decode_v5_batch(&wal, offset, crate::pitr::LIVE_WAL_V5_LIMITS)
                    .unwrap();
            expected.update(&wal[offset..decoded.data_end]);
            padding += decoded.logical_end - decoded.data_end;
            batches += 1;
            offset = decoded.logical_end;
        }
        // A fixture without padding cannot tell the two rules apart.
        assert_eq!(batches, 2);
        assert!(padding > 0, "expected alignment padding to strip");
        let expected: [u8; 32] = expected.finalize().into();

        let (seal, _) = build_v5_seal(&wal).unwrap();
        assert_eq!(seal.header.wal_format_version, crate::pitr::WAL_V5_VERSION);
        assert_eq!(seal.wal_digest, expected);
        // And it is not the whole-file hash, which is what the legacy rule would give.
        assert_ne!(seal.wal_digest, <[u8; 32]>::from(Sha256::digest(&wal)));
        // The legacy rule is refused on this file rather than silently answering.
        assert!(wal_digest(&wal, crate::pitr::WalDigestRule::WholeAlignedPrefix).is_err());
    }

    /// A v5 segment written by the tree *before* the logical-batch digest existed,
    /// committed byte for byte at `src/tests/fixtures/pitr-v5-segment.{wal,seal}`.
    ///
    /// A pinned digest guards a digest function; a pinned file guards the encoder,
    /// the version the segment carries, the rule that version selects, the seal
    /// echo and every reader at once. Segments written before the change verify
    /// forever, so this is the test that must fail first if that stops being true.
    #[test]
    fn legacy_segment_fixture_still_verifies_under_its_own_rule() {
        let wal = fixture("pitr-v5-segment.wal");
        let frozen_seal = fixture("pitr-v5-segment.seal");
        assert_eq!(
            hex(&Sha256::digest(&wal)),
            "4b6c684db2fb59eb0b9febfafa6f371c7f379e52f6c52179d966dd28ef92e03b"
        );
        assert_eq!(
            hex(&Sha256::digest(&frozen_seal)),
            "173aa1f4c3114d6a05427d666753612af8295eafc7ee0f3284cd8a3352b55542"
        );

        // Rebuilding reproduces the frozen seal rather than merely agreeing with it:
        // same digest, same entries, same logical length, and a seal that encodes to
        // the same bytes.
        let (seal, seal_bytes) = build_v5_seal(&wal).unwrap();
        assert_eq!(seal_bytes, frozen_seal);
        assert_eq!(
            seal.header.wal_format_version,
            crate::pitr::WAL_V5_VERSION_LEGACY
        );
        assert_eq!(seal.logical_length, 12288);
        assert_eq!(seal.entries.len(), 2);
        // The echo the reader trusts is the segment's own version, so the legacy
        // rule is selected by the file and nobody has to be told which to use.
        assert_eq!(
            crate::pitr::wal_digest_rule(seal.header.wal_format_version).unwrap(),
            crate::pitr::WalDigestRule::WholeAlignedPrefix
        );

        // Under its own rule the digest is the whole file's, exactly as before.
        assert_eq!(
            wal_digest(&wal, crate::pitr::WalDigestRule::WholeAlignedPrefix).unwrap(),
            <[u8; 32]>::from(Sha256::digest(&wal))
        );
        // Under the new rule it is refused, not silently different: a caller that
        // reads the rule from anywhere but the file gets a named error.
        let error = wal_digest(&wal, crate::pitr::WalDigestRule::LogicalBatches).unwrap_err();
        assert!(
            error.to_string().contains("does not match"),
            "unexpected error: {error}"
        );

        // What the new rule would skip here: the batches' own bytes against a file
        // that is two thirds alignment padding.
        let mut logical = WAL_V5_HEADER_LEN;
        let mut offset = WAL_V5_HEADER_LEN;
        while offset < wal.len() {
            let decoded =
                crate::pitr::decode_v5_batch(&wal, offset, crate::pitr::LIVE_WAL_V5_LIMITS)
                    .unwrap();
            logical += decoded.data_end - offset;
            offset = decoded.logical_end;
        }
        assert_eq!(logical, 4232);
        assert_eq!(wal.len(), 12288);
    }

    #[test]
    fn empty_header_seal_round_trips() {
        let header = WalV5Header {
            wal_format_version: crate::pitr::WAL_V5_VERSION,
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
                wal_format_version: crate::pitr::WAL_V5_VERSION,
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
            wal_format_version: crate::pitr::WAL_V5_VERSION,
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

    #[test]
    fn v7_seal_round_trips_and_verifies_ticket_offsets() -> Result<()> {
        let batches = [
            (
                10,
                RecordedAt {
                    secs: 100,
                    nanos: 2,
                },
            ),
            (
                20,
                RecordedAt {
                    secs: 101,
                    nanos: 3,
                },
            ),
        ];
        let (wal, header, active) = v7_fixture(&batches)?;
        let (seal, bytes) = build_v7_seal(&wal, active, header.predecessor)?;

        assert_eq!(seal.entries.len(), 2);
        assert_eq!(seal.entries[0].ticket, 0);
        assert_eq!(
            seal.entries[0].first_data_offset,
            2 * WAL_V7_FRAME_LEN as u64
        );
        assert_eq!(seal.entries[0].commit_ts, 10);
        assert_eq!(seal.entries[1].ticket, 1);
        assert_eq!(
            seal.entries[1].first_data_offset,
            4 * WAL_V7_FRAME_LEN as u64
        );
        assert_eq!(seal.entries[1].commit_ts, 20);
        assert_eq!(&bytes[..8], V7_MAGIC);
        assert_eq!(u16::from_be_bytes(bytes[8..10].try_into()?), V7_VERSION);
        assert_eq!(
            u16::from_be_bytes(bytes[10..12].try_into()?) as usize,
            V7_HEADER_BYTES
        );
        assert_eq!(
            u16::from_be_bytes(bytes[12..14].try_into()?),
            crate::wal::format::WAL_FORMAT_VERSION_V7
        );
        assert_eq!(u64::from_be_bytes(bytes[152..160].try_into()?), 2);
        assert_eq!(
            u64::from_be_bytes(bytes[160..168].try_into()?),
            active.durable_end
        );
        assert_eq!(
            u64::from_be_bytes(bytes[V7_HEADER_BYTES..V7_HEADER_BYTES + 8].try_into()?),
            0
        );
        assert_eq!(
            u64::from_be_bytes(bytes[V7_HEADER_BYTES + 8..V7_HEADER_BYTES + 16].try_into()?),
            2 * WAL_V7_FRAME_LEN as u64
        );
        assert_eq!(V7Seal::decode(&bytes, crate::wal::MAX_WAL_FILE_SIZE)?, seal);
        seal.verify_wal(&wal)?;

        let mut wrong_index = seal.clone();
        // This points at the valid interleaved FRONTIER between the two batches.
        wrong_index.entries[0].first_data_offset = 3 * WAL_V7_FRAME_LEN as u64;
        assert!(wrong_index.encode().is_ok());
        assert!(wrong_index.verify_wal(&wal).is_err());

        let mut damaged_wal = wal.clone();
        damaged_wal[2 * WAL_V7_FRAME_LEN + 100] ^= 1;
        assert!(seal.verify_wal(&damaged_wal).is_err());
        Ok(())
    }

    #[test]
    fn empty_v7_seal_preserves_the_generation_zero_image() -> Result<()> {
        let (wal, header, active) = v7_fixture(&[])?;
        assert_eq!(wal.len(), 2 * WAL_V7_FRAME_LEN);
        let (seal, bytes) = build_v7_seal(&wal, active, header.predecessor)?;
        assert!(seal.entries.is_empty());
        assert_eq!(seal.boundary.sealed_end, 2 * WAL_V7_FRAME_LEN as u64);
        assert_eq!(V7Seal::decode(&bytes, crate::wal::MAX_WAL_FILE_SIZE)?, seal);
        seal.verify_wal(&wal)?;
        Ok(())
    }

    #[test]
    fn v7_seal_indexes_fragmented_data_before_a_retained_frontier() -> Result<()> {
        let (mut wal, header, empty_active) = v7_fixture(&[])?;
        let header_digest = header.digest()?;
        let batches = [
            crate::pitr::WalBatch {
                commit_ts: 10,
                recorded_at: RecordedAt { secs: 10, nanos: 0 },
                entries: vec![crate::pitr::WalEntry::Put {
                    key: b"first".to_vec(),
                    value: b"small".to_vec(),
                }],
            },
            crate::pitr::WalBatch {
                commit_ts: 20,
                recorded_at: RecordedAt { secs: 20, nanos: 0 },
                entries: vec![crate::pitr::WalEntry::Put {
                    key: b"second".to_vec(),
                    value: vec![0x5a; 2 * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY],
                }],
            },
        ];
        let mut logical_hasher = Sha256::new();
        logical_hasher.update(header.encode()?);
        let mut first_data_offsets = [0_u64; 2];
        let mut data_ends = [0_u64; 2];
        let mut prefix_digests = [[0_u8; 32]; 2];
        for (index, batch) in batches.iter().enumerate() {
            let ticket = u64::try_from(index)?;
            let logical = encode_logical_batch(ticket, batch, crate::pitr::LIVE_WAL_V5_LIMITS)?;
            let fragment_count =
                crate::wal::v7::codec::data_fragment_count(u64::try_from(logical.len())?)
                    .context("fragmented seal fixture has an invalid batch length")?;
            first_data_offsets[index] = u64::try_from(wal.len())?;
            let mut frames = vec![0; usize::try_from(fragment_count)? * WAL_V7_FRAME_LEN];
            assert_eq!(
                encode_data_frames_into(
                    header_digest,
                    first_data_offsets[index],
                    ticket,
                    &logical,
                    crate::pitr::LIVE_WAL_V5_LIMITS,
                    &mut frames,
                )?,
                frames.len()
            );
            wal.extend_from_slice(&frames);
            data_ends[index] = u64::try_from(wal.len())?;
            logical_hasher.update(&logical);
            prefix_digests[index] = logical_hasher.clone().finalize().into();
        }
        assert!(data_ends[1] - first_data_offsets[1] > WAL_V7_FRAME_LEN as u64);

        // D0 | fragmented D1 | F_A(D0) | F_B(D0,D1): F_B retains F_A,
        // even though all D1 fragments physically precede F_A.
        let retained_frontier_offset = u64::try_from(wal.len())?;
        let mut previous_frontier = WalV7Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: empty_active.durable_end,
            last_commit_ts: 0,
            prefix_digest: header_digest,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let mut previous_frontier_offset = WAL_V7_HEADER_LEN as u64;
        let mut previous_frontier_digest =
            frame_record_digest(&wal[WAL_V7_HEADER_LEN..2 * WAL_V7_FRAME_LEN])?;
        for (index, batch) in batches.iter().enumerate() {
            let frame_offset = u64::try_from(wal.len())?;
            let frontier = WalV7Frontier {
                generation: u64::try_from(index)? + 1,
                ticket_end: u64::try_from(index)? + 1,
                durable_end: data_ends[index]
                    .max(previous_frontier_offset + WAL_V7_FRAME_LEN as u64),
                last_commit_ts: batch.commit_ts,
                prefix_digest: prefix_digests[index],
                previous_frontier_offset,
                previous_frontier_digest,
            };
            let frame = encode_frontier_successor(
                frontier,
                previous_frontier,
                previous_frontier_offset,
                &previous_frontier_digest,
                frame_offset,
                header_digest,
            )?;
            previous_frontier = frontier;
            previous_frontier_offset = frame_offset;
            previous_frontier_digest = frame_record_digest(&frame)?;
            wal.extend_from_slice(&frame);
        }
        let active = ActiveBoundary {
            ticket_end: previous_frontier.ticket_end,
            durable_end: previous_frontier.durable_end,
            last_commit_ts: previous_frontier.last_commit_ts,
            prefix_digest: previous_frontier.prefix_digest,
            ..empty_active
        };
        assert!(retained_frontier_offset > data_ends[0]);
        assert_eq!(
            active.durable_end,
            retained_frontier_offset + WAL_V7_FRAME_LEN as u64
        );

        let (seal, bytes) = build_v7_seal(&wal, active, header.predecessor)?;
        assert_eq!(
            seal.entries,
            vec![
                V7SealEntry {
                    ticket: 0,
                    first_data_offset: first_data_offsets[0],
                    commit_ts: batches[0].commit_ts,
                    recorded_at: batches[0].recorded_at,
                },
                V7SealEntry {
                    ticket: 1,
                    first_data_offset: first_data_offsets[1],
                    commit_ts: batches[1].commit_ts,
                    recorded_at: batches[1].recorded_at,
                },
            ]
        );
        assert_eq!(V7Seal::decode(&bytes, seal.boundary.sealed_end)?, seal);
        seal.verify_wal(&wal)?;

        let limits = WalV5Limits {
            max_batch_data_bytes: 1024,
            ..crate::pitr::LIVE_WAL_V5_LIMITS
        };
        assert!(seal.decode_wal_batches(&wal, limits).is_err());
        let discovery = discover_frontier_candidates(
            &mut Cursor::new(wal.as_slice()),
            wal.len() as u64,
            &header_digest,
        )?;
        let mut reader = PrefixReader {
            cursor: Cursor::new(wal.as_slice()),
            read_limit: first_data_offsets[1] + WAL_V7_FRAME_LEN as u64,
            read_beyond_limit: false,
        };
        assert!(
            select_recovery_candidate_with_limits(
                &mut reader,
                wal.len() as u64,
                &discovery,
                WalV7RecoveryAuthority::Sealed {
                    predecessor: header.predecessor,
                    boundary: seal.boundary,
                },
                limits,
            )
            .is_err()
        );
        assert!(
            !reader.read_beyond_limit,
            "byte-limit rejection must precede reading continuation fragments"
        );

        // The replay walker must also reject from the first fragment's length,
        // before inspecting or copying a damaged continuation fragment.
        wal[usize::try_from(first_data_offsets[1])? + WAL_V7_FRAME_LEN] ^= 1;
        let error = walk_v7_seal_entries(&wal, header, limits, |_, _| Ok(())).unwrap_err();
        assert!(error.to_string().contains("exceeds configured byte limit"));
        Ok(())
    }

    #[test]
    fn v7_seal_rejects_corrupt_checksums_and_reserved_index_bytes() -> Result<()> {
        let (wal, header, active) = v7_fixture(&[(1, RecordedAt { secs: 1, nanos: 0 })])?;
        let (_, bytes) = build_v7_seal(&wal, active, header.predecessor)?;

        let mut corrupt_crc = bytes.clone();
        corrupt_crc[316] ^= 1;
        assert!(V7Seal::decode(&corrupt_crc, crate::wal::MAX_WAL_FILE_SIZE).is_err());

        let mut nonzero_reserved = bytes;
        nonzero_reserved[V7_HEADER_BYTES + 36] = 1;
        let index_crc = crc32fast::hash(&nonzero_reserved[V7_HEADER_BYTES..]);
        nonzero_reserved[312..316].copy_from_slice(&index_crc.to_be_bytes());
        let header_crc = crc32fast::hash(&nonzero_reserved[8..316]);
        nonzero_reserved[316..320].copy_from_slice(&header_crc.to_be_bytes());
        assert!(V7Seal::decode(&nonzero_reserved, crate::wal::MAX_WAL_FILE_SIZE).is_err());
        Ok(())
    }

    #[test]
    fn v7_seal_rejects_entry_count_length_overflow() -> Result<()> {
        let (wal, header, active) = v7_fixture(&[])?;
        let (_, mut bytes) = build_v7_seal(&wal, active, header.predecessor)?;
        let count = usize::MAX / V7_ENTRY_BYTES;
        let body_len = count * V7_ENTRY_BYTES;
        bytes[280..288].copy_from_slice(&u64::try_from(count)?.to_be_bytes());
        bytes[304..312].copy_from_slice(&u64::try_from(body_len)?.to_be_bytes());
        let index_crc = crc32fast::hash(&bytes[V7_HEADER_BYTES..]);
        bytes[312..316].copy_from_slice(&index_crc.to_be_bytes());
        let header_crc = crc32fast::hash(&bytes[8..316]);
        bytes[316..320].copy_from_slice(&header_crc.to_be_bytes());

        assert!(V7Seal::decode(&bytes, crate::wal::MAX_WAL_FILE_SIZE).is_err());
        Ok(())
    }

    #[test]
    fn v7_seal_rejects_entry_count_above_wal_limit_before_allocation() -> Result<()> {
        let (wal, header, active) = v7_fixture(&[])?;
        let (_, mut bytes) = build_v7_seal(&wal, active, header.predecessor)?;
        let count = V7_MAX_ENTRY_COUNT + 1;
        let body_len = count * V7_ENTRY_BYTES;
        bytes[280..288].copy_from_slice(&u64::try_from(count)?.to_be_bytes());
        bytes[304..312].copy_from_slice(&u64::try_from(body_len)?.to_be_bytes());
        let header_crc = crc32fast::hash(&bytes[8..316]);
        bytes[316..320].copy_from_slice(&header_crc.to_be_bytes());

        let error = V7Seal::decode(&bytes, crate::wal::MAX_WAL_FILE_SIZE)
            .expect_err("entry count exceeds the WAL limit");
        assert!(
            error
                .to_string()
                .contains("entry count exceeds the persisted segment limit")
        );

        let oversized = vec![0; V7_MAX_SEAL_BYTES + 1];
        let error = V7Seal::decode(&oversized, crate::wal::MAX_WAL_FILE_SIZE)
            .expect_err("sidecar exceeds the byte limit");
        assert!(
            error
                .to_string()
                .contains("persisted segment sidecar limit")
        );
        Ok(())
    }

    #[test]
    fn v7_seal_encoding_obeys_global_image_and_index_capacity() -> Result<()> {
        let (wal, header, active) = v7_fixture(&[(1, RecordedAt { secs: 1, nanos: 0 })])?;
        let (seal, _) = build_v7_seal(&wal, active, header.predecessor)?;

        let mut oversized_image = seal.clone();
        oversized_image.boundary.active.durable_end = crate::wal::MAX_WAL_FILE_SIZE;
        oversized_image.boundary.sealed_end =
            crate::wal::MAX_WAL_FILE_SIZE + WAL_V7_FRAME_LEN as u64;
        let error = oversized_image
            .encode()
            .expect_err("image exceeds the global WAL format limit");
        assert!(
            error
                .to_string()
                .contains("sealed image exceeds the WAL format limit")
        );

        let mut oversized_index = seal;
        oversized_index.boundary.active.ticket_end = 2;
        oversized_index.boundary.active.last_commit_ts = 2;
        oversized_index.entries.push(V7SealEntry {
            ticket: 1,
            first_data_offset: WAL_V7_HEADER_LEN as u64 + WAL_V7_FRAME_LEN as u64,
            commit_ts: 2,
            recorded_at: RecordedAt { secs: 2, nanos: 0 },
        });
        let error = oversized_index
            .encode()
            .expect_err("index has more tickets than its sealed image can contain");
        assert!(
            error
                .to_string()
                .contains("seal index exceeds its sealed image capacity")
        );
        Ok(())
    }

    #[test]
    fn v7_seal_decode_obeys_the_persisted_segment_limit() -> Result<()> {
        let (wal, header, active) = v7_fixture(&[(1, RecordedAt { secs: 1, nanos: 0 })])?;
        let (seal, bytes) = build_v7_seal(&wal, active, header.predecessor)?;
        let minimum_segment_limit = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64;
        let persisted_segment_limit = seal.boundary.sealed_end;

        let error = V7Seal::decode(&bytes, minimum_segment_limit)
            .expect_err("one-batch seal exceeds an empty-only persisted segment limit");
        assert!(
            error
                .to_string()
                .contains("persisted segment sidecar limit")
        );

        assert!(
            bytes.len() <= v7_max_seal_bytes(v7_max_entry_count(persisted_segment_limit)?)?,
            "the malformed sidecars below must pass the early persisted sidecar cap"
        );

        let mut sealed_end_over_limit = bytes.clone();
        sealed_end_over_limit[208..216]
            .copy_from_slice(&(persisted_segment_limit + WAL_V7_FRAME_LEN as u64).to_be_bytes());
        refresh_v7_seal_header_crc(&mut sealed_end_over_limit);
        let error = V7Seal::decode(&sealed_end_over_limit, persisted_segment_limit)
            .expect_err("sealed image exceeds the persisted segment limit");
        assert!(
            error
                .to_string()
                .contains("sealed image exceeds the persisted segment limit")
        );

        let mut index_exceeds_sealed_capacity = bytes.clone();
        index_exceeds_sealed_capacity[208..216]
            .copy_from_slice(&(2 * WAL_V7_FRAME_LEN as u64).to_be_bytes());
        refresh_v7_seal_header_crc(&mut index_exceeds_sealed_capacity);
        let error = V7Seal::decode(&index_exceeds_sealed_capacity, persisted_segment_limit)
            .expect_err("index count exceeds the claimed sealed image capacity");
        assert!(
            error
                .to_string()
                .contains("entry count exceeds its sealed image capacity")
        );

        assert_eq!(
            V7Seal::decode(&bytes, persisted_segment_limit)?,
            seal,
            "the exact sealed image length permits the one-batch seal"
        );
        Ok(())
    }
}
