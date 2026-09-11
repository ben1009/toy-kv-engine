//! Dormant PITR catalog v1 framing, canonical encoding, and replay validation.
//!
//! This module deliberately does not publish repository objects or mutate the
//! source manifest. It provides the bounded catalog substrate used by the
//! later archiver state machine.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail, ensure};
use sha2::{Digest, Sha256};

use crate::pitr::{ArchiveEpochId, ChainAnchor, SegmentAnchor, SegmentId, TimelineId};

const MAGIC: [u8; 4] = *b"PITR";
const VERSION: u16 = 1;
const FRAME_HEADER_BYTES: usize = 4 + 2 + 4 + 8;
const FRAME_TRAILER_BYTES: usize = 4;
const RECORD_DIGEST_BYTES: usize = 32;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECORDS: usize = 1_000_000;
const MAX_SEGMENTS_PER_SNAPSHOT: usize = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PitrCatalogLimits {
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_catalog_bytes: usize,
    pub(crate) max_records: usize,
    pub(crate) max_segments_per_snapshot: usize,
}

impl Default for PitrCatalogLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_catalog_bytes: MAX_CATALOG_BYTES,
            max_records: MAX_RECORDS,
            max_segments_per_snapshot: MAX_SEGMENTS_PER_SNAPSHOT,
        }
    }
}

impl PitrCatalogLimits {
    fn validate(self) -> Result<Self> {
        ensure!(
            self.max_frame_bytes >= FRAME_HEADER_BYTES + FRAME_TRAILER_BYTES + RECORD_DIGEST_BYTES,
            "catalog frame limit is too small"
        );
        ensure!(
            self.max_catalog_bytes >= self.max_frame_bytes,
            "catalog byte limit is below frame limit"
        );
        ensure!(self.max_records > 0, "catalog record limit must be nonzero");
        ensure!(
            self.max_segments_per_snapshot > 0,
            "catalog snapshot segment limit must be nonzero"
        );
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SegmentKey {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: TimelineId,
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) segment_id: SegmentId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentMetadata {
    pub(crate) key: SegmentKey,
    pub(crate) wal_format_version: u16,
    pub(crate) seal_format_version: u16,
    pub(crate) anchor: SegmentAnchor,
    pub(crate) predecessor: ChainAnchor,
    pub(crate) first_commit_ts: Option<u64>,
    pub(crate) last_commit_ts: Option<u64>,
    pub(crate) batch_count: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) wal_bytes: u64,
    pub(crate) wal_digest: [u8; 32],
    pub(crate) seal_digest: [u8; 32],
    pub(crate) source_identity: [u8; 32],
}

impl SegmentMetadata {
    fn validate(&self) -> Result<()> {
        ensure!(self.wal_format_version == 5, "unsupported PITR WAL format");
        ensure!(
            self.seal_format_version == 1,
            "unsupported PITR seal format"
        );
        ensure!(
            self.anchor.segment_id == self.key.segment_id,
            "segment anchor identity mismatch"
        );
        ensure!(
            self.anchor.wal_digest == self.wal_digest,
            "segment WAL digest mismatch"
        );
        ensure!(
            self.anchor.seal_digest == self.seal_digest,
            "segment seal digest mismatch"
        );
        match (self.first_commit_ts, self.last_commit_ts) {
            (Some(first), Some(last)) => ensure!(first <= last, "segment commit range regressed"),
            (None, None) => ensure!(self.batch_count == 0, "empty segment has batches"),
            _ => bail!("segment commit range is incomplete"),
        }
        if self.batch_count == 0 {
            ensure!(
                self.first_commit_ts.is_none(),
                "empty segment has first commit"
            );
            ensure!(
                self.last_commit_ts.is_none(),
                "empty segment has last commit"
            );
        }
        if self.key.archive_epoch_id != self.anchor_epoch() {
            bail!("segment anchor epoch mismatch");
        }
        Ok(())
    }

    fn anchor_epoch(&self) -> ArchiveEpochId {
        match self.predecessor {
            ChainAnchor::Genesis { archive_epoch_id } => archive_epoch_id,
            ChainAnchor::Segment(_) => self.key.archive_epoch_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoverageBreakReason {
    SourceLoss,
    PublicationUnknown,
    RepositoryUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CoverageBreak {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: TimelineId,
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) after: ChainAnchor,
    pub(crate) first_uncovered_commit_ts: Option<u64>,
    pub(crate) reason: CoverageBreakReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RetentionSnapshot {
    pub(crate) repository_id: [u8; 16],
    pub(crate) segments: Vec<SegmentMetadata>,
    pub(crate) breaks: Vec<CoverageBreak>,
    pub(crate) backup_catalog_high_water: u64,
    pub(crate) backup_catalog_digest: [u8; 32],
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PitrCatalogRecord {
    CommitSegment { metadata: SegmentMetadata },
    CoverageBreak(CoverageBreak),
    RetentionSnapshot(RetentionSnapshot),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogReplay {
    pub(crate) records: Vec<PitrCatalogRecord>,
    pub(crate) sequence: u64,
    pub(crate) retained_offset: usize,
    pub(crate) prefix_digest: [u8; 32],
}

pub(crate) fn encode_catalog(records: &[PitrCatalogRecord]) -> Result<Vec<u8>> {
    encode_catalog_with_limits(records, PitrCatalogLimits::default())
}

pub(crate) fn encode_catalog_with_limits(
    records: &[PitrCatalogRecord],
    limits: PitrCatalogLimits,
) -> Result<Vec<u8>> {
    let limits = limits.validate()?;
    ensure!(
        records.len() <= limits.max_records,
        "catalog record limit exceeded"
    );
    let mut output = Vec::new();
    for (index, record) in records.iter().enumerate() {
        let sequence =
            u64::try_from(index + 1).map_err(|_| anyhow::anyhow!("catalog sequence exhausted"))?;
        let frame = encode_frame(sequence, record, limits.max_frame_bytes)?;
        ensure!(
            output.len() <= limits.max_catalog_bytes.saturating_sub(frame.len()),
            "catalog byte limit exceeded"
        );
        output.extend_from_slice(&frame);
    }
    Ok(output)
}

pub(crate) fn replay_catalog(input: &[u8]) -> Result<CatalogReplay> {
    replay_catalog_with_limits(input, PitrCatalogLimits::default())
}

pub(crate) fn replay_catalog_with_limits(
    input: &[u8],
    limits: PitrCatalogLimits,
) -> Result<CatalogReplay> {
    let limits = limits.validate()?;
    ensure!(
        input.len() <= limits.max_catalog_bytes,
        "catalog byte limit exceeded"
    );
    let mut records = Vec::new();
    let mut offset = 0;
    let mut expected_sequence = 1_u64;
    while offset < input.len() {
        let remaining = input.len() - offset;
        if remaining < FRAME_HEADER_BYTES {
            break;
        }
        ensure!(
            input[offset..offset + 4] == MAGIC,
            "invalid PITR catalog magic"
        );
        let version = u16::from_be_bytes(input[offset + 4..offset + 6].try_into().unwrap());
        ensure!(version == VERSION, "unsupported PITR catalog version");
        let payload_len = usize::try_from(u32::from_be_bytes(
            input[offset + 6..offset + 10].try_into().unwrap(),
        ))
        .map_err(|_| anyhow::anyhow!("catalog frame length overflow"))?;
        ensure!(
            payload_len
                <= limits
                    .max_frame_bytes
                    .saturating_sub(FRAME_HEADER_BYTES + FRAME_TRAILER_BYTES),
            "catalog frame exceeds limit"
        );
        let sequence = u64::from_be_bytes(input[offset + 10..offset + 18].try_into().unwrap());
        let frame_len = FRAME_HEADER_BYTES
            .checked_add(payload_len)
            .and_then(|length| length.checked_add(FRAME_TRAILER_BYTES))
            .ok_or_else(|| anyhow::anyhow!("catalog frame length overflow"))?;
        if remaining < frame_len {
            break;
        }
        ensure!(
            sequence == expected_sequence,
            "catalog sequence is not contiguous"
        );
        let payload_start = offset + FRAME_HEADER_BYTES;
        let payload_end = payload_start + payload_len;
        let payload = &input[payload_start..payload_end];
        let stored_crc =
            u32::from_be_bytes(input[payload_end..payload_end + 4].try_into().unwrap());
        let mut crc_input = Vec::with_capacity(2 + 4 + 8 + payload_len);
        crc_input.extend_from_slice(&version.to_be_bytes());
        crc_input.extend_from_slice(&(payload_len as u32).to_be_bytes());
        crc_input.extend_from_slice(&sequence.to_be_bytes());
        crc_input.extend_from_slice(payload);
        ensure!(
            crc32fast::hash(&crc_input) == stored_crc,
            "PITR catalog frame checksum mismatch"
        );
        let record = decode_record(payload, limits.max_segments_per_snapshot)?;
        validate_record(&record)?;
        records.push(record);
        ensure!(
            records.len() <= limits.max_records,
            "catalog record limit exceeded"
        );
        offset += frame_len;
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("catalog sequence exhausted"))?;
    }
    validate_replay(&records)?;
    let mut digest = Sha256::new();
    digest.update(&input[..offset]);
    Ok(CatalogReplay {
        sequence: expected_sequence - 1,
        records,
        retained_offset: offset,
        prefix_digest: digest.finalize().into(),
    })
}

fn encode_frame(
    sequence: u64,
    record: &PitrCatalogRecord,
    max_frame_bytes: usize,
) -> Result<Vec<u8>> {
    let body = encode_record(record)?;
    let mut payload = Vec::with_capacity(1 + body.len() + RECORD_DIGEST_BYTES);
    payload.push(record_tag(record));
    payload.extend_from_slice(&body);
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    payload.extend_from_slice(&digest);
    ensure!(
        payload.len() <= max_frame_bytes.saturating_sub(FRAME_HEADER_BYTES + FRAME_TRAILER_BYTES),
        "catalog frame exceeds limit"
    );
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| anyhow::anyhow!("catalog payload exceeds wire format"))?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len() + FRAME_TRAILER_BYTES);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&VERSION.to_be_bytes());
    frame.extend_from_slice(&payload_len.to_be_bytes());
    frame.extend_from_slice(&sequence.to_be_bytes());
    frame.extend_from_slice(&payload);
    let crc = crc32fast::hash(&frame[4..]);
    frame.extend_from_slice(&crc.to_be_bytes());
    Ok(frame)
}

fn record_tag(record: &PitrCatalogRecord) -> u8 {
    match record {
        PitrCatalogRecord::CommitSegment { .. } => 1,
        PitrCatalogRecord::CoverageBreak(_) => 2,
        PitrCatalogRecord::RetentionSnapshot(_) => 3,
    }
}

fn encode_record(record: &PitrCatalogRecord) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match record {
        PitrCatalogRecord::CommitSegment { metadata } => encode_metadata(&mut out, metadata)?,
        PitrCatalogRecord::CoverageBreak(break_record) => encode_break(&mut out, break_record)?,
        PitrCatalogRecord::RetentionSnapshot(snapshot) => {
            put_bytes(&mut out, &snapshot.repository_id);
            put_u64(
                &mut out,
                u64::try_from(snapshot.segments.len())
                    .map_err(|_| anyhow::anyhow!("snapshot segment count overflow"))?,
            );
            for metadata in &snapshot.segments {
                encode_metadata(&mut out, metadata)?;
            }
            put_u64(
                &mut out,
                u64::try_from(snapshot.breaks.len())
                    .map_err(|_| anyhow::anyhow!("snapshot break count overflow"))?,
            );
            for break_record in &snapshot.breaks {
                encode_break(&mut out, break_record)?;
            }
            put_u64(&mut out, snapshot.backup_catalog_high_water);
            put_bytes(&mut out, &snapshot.backup_catalog_digest);
        }
    }
    Ok(out)
}

fn decode_record(input: &[u8], max_segments: usize) -> Result<PitrCatalogRecord> {
    ensure!(
        input.len() > RECORD_DIGEST_BYTES,
        "catalog record is truncated"
    );
    let split = input.len() - RECORD_DIGEST_BYTES;
    let body = &input[..split];
    let expected: [u8; 32] = Sha256::digest(body).into();
    ensure!(input[split..] == expected, "catalog record digest mismatch");
    let mut cursor = Cursor::new(body);
    let tag = cursor.u8()?;
    let record_body = cursor.rest();
    let record = match tag {
        1 => PitrCatalogRecord::CommitSegment {
            metadata: decode_metadata(record_body)?,
        },
        2 => PitrCatalogRecord::CoverageBreak(decode_break(record_body)?),
        3 => {
            let mut cursor = Cursor::new(record_body);
            let repository_id = cursor.fixed::<16>()?;
            let segment_count = cursor.usize()?;
            ensure!(
                segment_count <= max_segments,
                "snapshot segment limit exceeded"
            );
            let mut segments = Vec::with_capacity(segment_count);
            for _ in 0..segment_count {
                let (metadata, used) = decode_metadata_prefix(cursor.rest())?;
                cursor.advance(used)?;
                segments.push(metadata);
            }
            let break_count = cursor.usize()?;
            ensure!(break_count <= max_segments, "snapshot break limit exceeded");
            let mut breaks = Vec::with_capacity(break_count);
            for _ in 0..break_count {
                let (break_record, used) = decode_break_prefix(cursor.rest())?;
                cursor.advance(used)?;
                breaks.push(break_record);
            }
            let backup_catalog_high_water = cursor.u64()?;
            let backup_catalog_digest = cursor.fixed::<32>()?;
            cursor.finish()?;
            PitrCatalogRecord::RetentionSnapshot(RetentionSnapshot {
                repository_id,
                segments,
                breaks,
                backup_catalog_high_water,
                backup_catalog_digest,
            })
        }
        _ => bail!("unknown PITR catalog record tag"),
    };
    Ok(record)
}

fn validate_record(record: &PitrCatalogRecord) -> Result<()> {
    match record {
        PitrCatalogRecord::CommitSegment { metadata } => metadata.validate(),
        PitrCatalogRecord::CoverageBreak(break_record) => validate_break(break_record),
        PitrCatalogRecord::RetentionSnapshot(snapshot) => {
            ensure!(
                snapshot.segments.len() <= MAX_SEGMENTS_PER_SNAPSHOT,
                "snapshot segment limit exceeded"
            );
            for metadata in &snapshot.segments {
                metadata.validate()?;
            }
            for break_record in &snapshot.breaks {
                validate_break(break_record)?;
            }
            Ok(())
        }
    }
}

fn validate_break(break_record: &CoverageBreak) -> Result<()> {
    let _ = break_record.first_uncovered_commit_ts;
    Ok(())
}

fn validate_replay(records: &[PitrCatalogRecord]) -> Result<()> {
    let mut segments = HashMap::<SegmentKey, SegmentMetadata>::new();
    let mut broken = HashSet::<(TimelineId, ArchiveEpochId)>::new();
    let mut last_by_chain = HashMap::<(TimelineId, ArchiveEpochId), SegmentAnchor>::new();
    let mut repository_id = None;
    for record in records {
        match record {
            PitrCatalogRecord::CommitSegment { metadata } => {
                if let Some(expected) = repository_id {
                    ensure!(
                        expected == metadata.key.repository_id,
                        "catalog repository identity changed"
                    );
                } else {
                    repository_id = Some(metadata.key.repository_id);
                }
                let chain = (metadata.key.timeline_id, metadata.key.archive_epoch_id);
                ensure!(
                    !broken.contains(&chain),
                    "segment committed after coverage break"
                );
                if let Some(previous) = last_by_chain.get(&chain) {
                    ensure!(
                        metadata.predecessor == ChainAnchor::Segment(*previous),
                        "segment predecessor chain mismatch"
                    );
                } else {
                    ensure!(
                        metadata.predecessor
                            == ChainAnchor::Genesis {
                                archive_epoch_id: metadata.key.archive_epoch_id
                            },
                        "first segment must use epoch genesis"
                    );
                }
                if let Some(existing) = segments.insert(metadata.key, metadata.clone()) {
                    ensure!(
                        existing == *metadata,
                        "conflicting duplicate segment metadata"
                    );
                    bail!("duplicate segment commit");
                }
                last_by_chain.insert(chain, metadata.anchor);
            }
            PitrCatalogRecord::CoverageBreak(break_record) => {
                if let Some(expected) = repository_id {
                    ensure!(
                        expected == break_record.repository_id,
                        "catalog repository identity changed"
                    );
                } else {
                    repository_id = Some(break_record.repository_id);
                }
                let chain = (break_record.timeline_id, break_record.archive_epoch_id);
                ensure!(!broken.insert(chain), "duplicate coverage break");
                if let Some(previous) = last_by_chain.get(&chain) {
                    ensure!(
                        break_record.after == ChainAnchor::Segment(*previous),
                        "coverage break predecessor mismatch"
                    );
                } else {
                    ensure!(
                        break_record.after
                            == ChainAnchor::Genesis {
                                archive_epoch_id: break_record.archive_epoch_id
                            },
                        "coverage break has no matching predecessor"
                    );
                }
            }
            PitrCatalogRecord::RetentionSnapshot(snapshot) => {
                if let Some(expected) = repository_id {
                    ensure!(
                        expected == snapshot.repository_id,
                        "catalog repository identity changed"
                    );
                } else {
                    repository_id = Some(snapshot.repository_id);
                }
                ensure!(
                    snapshot.segments.len() <= MAX_SEGMENTS_PER_SNAPSHOT,
                    "snapshot segment limit exceeded"
                );
                segments.clear();
                broken.clear();
                last_by_chain.clear();
                for metadata in &snapshot.segments {
                    ensure!(
                        metadata.key.repository_id == snapshot.repository_id,
                        "snapshot repository mismatch"
                    );
                    let chain = (metadata.key.timeline_id, metadata.key.archive_epoch_id);
                    ensure!(
                        !broken.contains(&chain),
                        "snapshot segment follows coverage break"
                    );
                    if let Some(previous) = last_by_chain.get(&chain) {
                        ensure!(
                            metadata.predecessor == ChainAnchor::Segment(*previous),
                            "snapshot segment predecessor chain mismatch"
                        );
                    } else {
                        ensure!(
                            metadata.predecessor
                                == ChainAnchor::Genesis {
                                    archive_epoch_id: metadata.key.archive_epoch_id
                                },
                            "snapshot first segment must use epoch genesis"
                        );
                    }
                    ensure!(
                        segments.insert(metadata.key, metadata.clone()).is_none(),
                        "snapshot contains duplicate segment"
                    );
                    last_by_chain.insert(chain, metadata.anchor);
                }
                for break_record in &snapshot.breaks {
                    ensure!(
                        break_record.repository_id == snapshot.repository_id,
                        "snapshot break repository mismatch"
                    );
                    let chain = (break_record.timeline_id, break_record.archive_epoch_id);
                    ensure!(
                        !broken.insert(chain),
                        "snapshot contains duplicate coverage break"
                    );
                    if let Some(previous) = last_by_chain.get(&chain) {
                        ensure!(
                            break_record.after == ChainAnchor::Segment(*previous),
                            "snapshot coverage break predecessor mismatch"
                        );
                    } else {
                        ensure!(
                            break_record.after
                                == ChainAnchor::Genesis {
                                    archive_epoch_id: break_record.archive_epoch_id
                                },
                            "snapshot coverage break has no matching predecessor"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn encode_metadata(out: &mut Vec<u8>, metadata: &SegmentMetadata) -> Result<()> {
    put_bytes(out, &metadata.key.repository_id);
    put_bytes(out, &metadata.key.timeline_id.0);
    put_bytes(out, &metadata.key.archive_epoch_id.0);
    put_u64(out, metadata.key.segment_id.0);
    put_u16(out, metadata.wal_format_version);
    put_u16(out, metadata.seal_format_version);
    encode_anchor(out, &metadata.anchor);
    encode_chain_anchor(out, &metadata.predecessor);
    put_optional_u64(out, metadata.first_commit_ts);
    put_optional_u64(out, metadata.last_commit_ts);
    put_u64(out, metadata.batch_count);
    put_u64(out, metadata.logical_bytes);
    put_u64(out, metadata.wal_bytes);
    put_bytes(out, &metadata.wal_digest);
    put_bytes(out, &metadata.seal_digest);
    put_bytes(out, &metadata.source_identity);
    Ok(())
}

fn decode_metadata(input: &[u8]) -> Result<SegmentMetadata> {
    let (metadata, used) = decode_metadata_prefix(input)?;
    ensure!(used == input.len(), "trailing bytes in segment metadata");
    Ok(metadata)
}

fn decode_metadata_prefix(input: &[u8]) -> Result<(SegmentMetadata, usize)> {
    let mut cursor = Cursor::new(input);
    let key = SegmentKey {
        repository_id: cursor.fixed()?,
        timeline_id: TimelineId(cursor.fixed()?),
        archive_epoch_id: ArchiveEpochId(cursor.fixed()?),
        segment_id: SegmentId(cursor.u64()?),
    };
    let metadata = SegmentMetadata {
        key,
        wal_format_version: cursor.u16()?,
        seal_format_version: cursor.u16()?,
        anchor: decode_anchor(&mut cursor)?,
        predecessor: decode_chain_anchor(&mut cursor)?,
        first_commit_ts: cursor.optional_u64()?,
        last_commit_ts: cursor.optional_u64()?,
        batch_count: cursor.u64()?,
        logical_bytes: cursor.u64()?,
        wal_bytes: cursor.u64()?,
        wal_digest: cursor.fixed()?,
        seal_digest: cursor.fixed()?,
        source_identity: cursor.fixed()?,
    };
    Ok((metadata, cursor.position()))
}

fn encode_break(out: &mut Vec<u8>, break_record: &CoverageBreak) -> Result<()> {
    put_bytes(out, &break_record.repository_id);
    put_bytes(out, &break_record.timeline_id.0);
    put_bytes(out, &break_record.archive_epoch_id.0);
    encode_chain_anchor(out, &break_record.after);
    put_optional_u64(out, break_record.first_uncovered_commit_ts);
    out.push(match break_record.reason {
        CoverageBreakReason::SourceLoss => 1,
        CoverageBreakReason::PublicationUnknown => 2,
        CoverageBreakReason::RepositoryUnavailable => 3,
    });
    Ok(())
}

fn decode_break(input: &[u8]) -> Result<CoverageBreak> {
    let (break_record, used) = decode_break_prefix(input)?;
    ensure!(used == input.len(), "trailing bytes in coverage break");
    Ok(break_record)
}

fn decode_break_prefix(input: &[u8]) -> Result<(CoverageBreak, usize)> {
    let mut cursor = Cursor::new(input);
    let break_record = CoverageBreak {
        repository_id: cursor.fixed()?,
        timeline_id: TimelineId(cursor.fixed()?),
        archive_epoch_id: ArchiveEpochId(cursor.fixed()?),
        after: decode_chain_anchor(&mut cursor)?,
        first_uncovered_commit_ts: cursor.optional_u64()?,
        reason: match cursor.u8()? {
            1 => CoverageBreakReason::SourceLoss,
            2 => CoverageBreakReason::PublicationUnknown,
            3 => CoverageBreakReason::RepositoryUnavailable,
            _ => bail!("unknown coverage break reason"),
        },
    };
    Ok((break_record, cursor.position()))
}

fn encode_anchor(out: &mut Vec<u8>, anchor: &SegmentAnchor) {
    put_u64(out, anchor.segment_id.0);
    put_bytes(out, &anchor.wal_digest);
    put_bytes(out, &anchor.seal_digest);
}

fn decode_anchor(cursor: &mut Cursor<'_>) -> Result<SegmentAnchor> {
    Ok(SegmentAnchor {
        segment_id: SegmentId(cursor.u64()?),
        wal_digest: cursor.fixed()?,
        seal_digest: cursor.fixed()?,
    })
}

fn encode_chain_anchor(out: &mut Vec<u8>, anchor: &ChainAnchor) {
    match anchor {
        ChainAnchor::Genesis { archive_epoch_id } => {
            out.push(0);
            put_bytes(out, &archive_epoch_id.0);
        }
        ChainAnchor::Segment(anchor) => {
            out.push(1);
            encode_anchor(out, anchor);
        }
    }
}

fn decode_chain_anchor(cursor: &mut Cursor<'_>) -> Result<ChainAnchor> {
    match cursor.u8()? {
        0 => Ok(ChainAnchor::Genesis {
            archive_epoch_id: ArchiveEpochId(cursor.fixed()?),
        }),
        1 => Ok(ChainAnchor::Segment(decode_anchor(cursor)?)),
        _ => bail!("unknown chain anchor tag"),
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes);
}
fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            put_u64(out, value);
        }
        None => out.push(0),
    }
}

struct Cursor<'a> {
    input: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn rest(&self) -> &'a [u8] {
        &self.input[self.position..]
    }

    fn advance(&mut self, count: usize) -> Result<()> {
        ensure!(count <= self.rest().len(), "truncated catalog record");
        self.position += count;
        Ok(())
    }

    fn bytes(&mut self, count: usize) -> Result<&'a [u8]> {
        let bytes = self
            .rest()
            .get(..count)
            .ok_or_else(|| anyhow::anyhow!("truncated catalog record"))?;
        self.position += count;
        Ok(bytes)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.bytes(N)?.try_into().unwrap())
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.fixed()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.fixed()?))
    }

    fn usize(&mut self) -> Result<usize> {
        usize::try_from(self.u64()?).map_err(|_| anyhow::anyhow!("catalog count overflows usize"))
    }

    fn optional_u64(&mut self) -> Result<Option<u64>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => bail!("invalid optional catalog value"),
        }
    }

    fn finish(&self) -> Result<()> {
        ensure!(
            self.position == self.input.len(),
            "trailing bytes in catalog record"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(segment_id: u64, predecessor: ChainAnchor) -> SegmentMetadata {
        let wal_digest = [segment_id as u8; 32];
        let seal_digest = [segment_id as u8 + 1; 32];
        SegmentMetadata {
            key: SegmentKey {
                repository_id: [7; 16],
                timeline_id: TimelineId([8; 16]),
                archive_epoch_id: ArchiveEpochId([9; 16]),
                segment_id: SegmentId(segment_id),
            },
            wal_format_version: 5,
            seal_format_version: 1,
            anchor: SegmentAnchor {
                segment_id: SegmentId(segment_id),
                wal_digest,
                seal_digest,
            },
            predecessor,
            first_commit_ts: Some(segment_id * 10),
            last_commit_ts: Some(segment_id * 10 + 9),
            batch_count: 1,
            logical_bytes: 100,
            wal_bytes: 120,
            wal_digest,
            seal_digest,
            source_identity: [segment_id as u8 + 2; 32],
        }
    }

    #[test]
    fn catalog_round_trips_and_replays_chain() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let second = metadata(2, ChainAnchor::Segment(first.anchor));
        let records = vec![
            PitrCatalogRecord::CommitSegment { metadata: first },
            PitrCatalogRecord::CommitSegment { metadata: second },
        ];
        let encoded = encode_catalog(&records).unwrap();
        let replay = replay_catalog(&encoded).unwrap();
        assert_eq!(replay.records, records);
        assert_eq!(replay.retained_offset, encoded.len());
        assert_eq!(replay.sequence, 2);
    }

    #[test]
    fn terminal_torn_frame_is_ignored_but_complete_corruption_fails() {
        let record = PitrCatalogRecord::CommitSegment {
            metadata: metadata(
                1,
                ChainAnchor::Genesis {
                    archive_epoch_id: ArchiveEpochId([9; 16]),
                },
            ),
        };
        let encoded = encode_catalog(&[record]).unwrap();
        let replay = replay_catalog(&encoded[..encoded.len() - 2]).unwrap();
        assert!(replay.records.is_empty());
        let mut corrupt = encoded;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(replay_catalog(&corrupt).is_err());
    }

    #[test]
    fn duplicate_and_post_break_segments_are_rejected() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let duplicate = PitrCatalogRecord::CommitSegment {
            metadata: first.clone(),
        };
        assert!(
            replay_catalog(
                &encode_catalog(&[
                    PitrCatalogRecord::CommitSegment {
                        metadata: first.clone()
                    },
                    duplicate
                ])
                .unwrap()
            )
            .is_err()
        );
        let break_record = PitrCatalogRecord::CoverageBreak(CoverageBreak {
            repository_id: [7; 16],
            timeline_id: TimelineId([8; 16]),
            archive_epoch_id: ArchiveEpochId([9; 16]),
            after: ChainAnchor::Segment(first.anchor),
            first_uncovered_commit_ts: Some(20),
            reason: CoverageBreakReason::SourceLoss,
        });
        let later = metadata(2, ChainAnchor::Segment(first.anchor));
        assert!(
            replay_catalog(
                &encode_catalog(&[
                    PitrCatalogRecord::CommitSegment { metadata: first },
                    break_record,
                    PitrCatalogRecord::CommitSegment { metadata: later }
                ])
                .unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn retention_snapshot_replaces_replay_chain() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let second = metadata(2, ChainAnchor::Segment(first.anchor));
        let snapshot = PitrCatalogRecord::RetentionSnapshot(RetentionSnapshot {
            repository_id: [7; 16],
            segments: vec![first],
            breaks: Vec::new(),
            backup_catalog_high_water: 4,
            backup_catalog_digest: [6; 32],
        });
        let replay = replay_catalog(
            &encode_catalog(&[
                snapshot,
                PitrCatalogRecord::CommitSegment { metadata: second },
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(replay.sequence, 2);
    }
}
