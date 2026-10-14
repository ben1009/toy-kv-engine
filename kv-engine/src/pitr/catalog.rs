//! PITR catalog v1 framing, canonical encoding, and replay validation.
//!
//! This module deliberately does not publish repository objects or mutate the
//! source manifest. It provides the bounded catalog substrate used by the
//! later archiver state machine.
#![allow(dead_code)]

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use anyhow::{Result, bail, ensure};
use sha2::{Digest, Sha256};

use crate::pitr::manifest::ImmutableBoundary;
use crate::pitr::{ArchiveEpochId, ChainAnchor, RecordedAt, SegmentAnchor, SegmentId, TimelineId};

const MAGIC: [u8; 4] = *b"PITR";
const VERSION: u16 = 1;
const FRAME_HEADER_BYTES: usize = 4 + 2 + 4 + 8 + 4;
const FRAME_TRAILER_BYTES: usize = 4;
const RECORD_DIGEST_BYTES: usize = 32;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECORDS: usize = 1_000_000;
const MAX_SNAPSHOT_ITEMS: usize = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PitrCatalogLimits {
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_catalog_bytes: usize,
    pub(crate) max_records: usize,
    pub(crate) max_decoded_state_bytes: usize,
    pub(crate) max_snapshot_items: usize,
}

impl Default for PitrCatalogLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_catalog_bytes: MAX_CATALOG_BYTES,
            max_records: MAX_RECORDS,
            max_decoded_state_bytes: MAX_CATALOG_BYTES,
            max_snapshot_items: MAX_SNAPSHOT_ITEMS,
        }
    }
}

impl PitrCatalogLimits {
    fn validate(self) -> Result<Self> {
        ensure!(
            self.max_frame_bytes <= MAX_FRAME_BYTES,
            "catalog frame limit exceeds protocol cap"
        );
        ensure!(
            self.max_catalog_bytes <= MAX_CATALOG_BYTES,
            "catalog byte limit exceeds protocol cap"
        );
        ensure!(
            self.max_records <= MAX_RECORDS,
            "catalog record limit exceeds protocol cap"
        );
        ensure!(
            self.max_decoded_state_bytes <= MAX_CATALOG_BYTES,
            "catalog decoded-state limit exceeds protocol cap"
        );
        ensure!(
            self.max_snapshot_items <= MAX_SNAPSHOT_ITEMS,
            "catalog snapshot limit exceeds protocol cap"
        );
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
            self.max_decoded_state_bytes > 0,
            "catalog decoded-state limit must be nonzero"
        );
        ensure!(
            self.max_snapshot_items > 0,
            "catalog snapshot limit must be nonzero"
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
    /// The frozen physical boundary for v7 segments. Legacy v5/v6 catalog
    /// entries keep this absent and retain their original wire representation.
    pub(crate) immutable_boundary: Option<Box<ImmutableBoundary>>,
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
    /// The rule this segment's `wal_digest` follows. Taken from the segment's own
    /// recorded version, so a repository holding segments from before the
    /// logical-batch digest keeps verifying each under the rule it was written with.
    pub(crate) fn wal_digest_rule(&self) -> Result<crate::pitr::WalDigestRule> {
        crate::pitr::wal_digest_rule(self.wal_format_version)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match (
            crate::pitr::is_v5_family(self.wal_format_version),
            self.wal_format_version == crate::wal::format::WAL_FORMAT_VERSION_V7,
            self.seal_format_version,
            self.immutable_boundary.as_deref(),
        ) {
            (true, false, 1, None) => {}
            (false, true, 2, Some(boundary)) => {
                self.validate_v7_boundary(boundary)?;
            }
            _ => bail!("unsupported PITR WAL/seal descriptor"),
        }
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

    fn validate_v7_boundary(&self, boundary: &ImmutableBoundary) -> Result<()> {
        use crate::wal::v7::codec::{WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WalV7Header};

        boundary.validate()?;
        let active = boundary.active;
        ensure!(
            active.timeline_id == self.key.timeline_id.0
                && active.archive_epoch_id == self.key.archive_epoch_id.0
                && active.segment_id == self.key.segment_id.0,
            "v7 catalog boundary identity mismatch"
        );
        if let ChainAnchor::Segment(predecessor) = self.predecessor {
            ensure!(
                predecessor.wal_digest != [0; 32] && predecessor.seal_digest != [0; 32],
                "v7 catalog predecessor digest is empty"
            );
        }
        let expected_sealed_end = active
            .durable_end
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("v7 catalog sealed image length overflows"))?;
        ensure!(
            boundary.sealed_end == expected_sealed_end,
            "v7 catalog image does not end immediately after its terminal FRONTIER"
        );
        ensure!(
            boundary.sealed_end <= crate::wal::MAX_WAL_FILE_SIZE,
            "v7 catalog sealed image exceeds the WAL format limit"
        );
        // Even a minimal nonempty image needs the header, generation-zero
        // FRONTIER, terminal FRONTIER, and at least one DATA frame per ticket.
        let fixed_bytes = (WAL_V7_HEADER_LEN + 2 * WAL_V7_FRAME_LEN) as u64;
        let max_batch_count =
            boundary.sealed_end.saturating_sub(fixed_bytes) / WAL_V7_FRAME_LEN as u64;
        ensure!(
            self.batch_count <= max_batch_count,
            "v7 catalog batch count exceeds its sealed image capacity"
        );
        ensure!(
            active.ticket_end == self.batch_count,
            "v7 catalog batch count differs from its durable ticket boundary"
        );
        ensure!(
            active.last_commit_ts == self.last_commit_ts.unwrap_or_default(),
            "v7 catalog timestamp differs from its durable boundary"
        );
        ensure!(
            self.first_commit_ts.is_none_or(|first| first != 0),
            "v7 catalog first commit timestamp is zero"
        );
        if let (Some(first), Some(last)) = (self.first_commit_ts, self.last_commit_ts) {
            let timestamp_capacity = last
                .checked_sub(first)
                .and_then(|span| span.checked_add(1))
                .ok_or_else(|| anyhow::anyhow!("v7 catalog commit range is invalid"))?;
            ensure!(
                self.batch_count <= timestamp_capacity,
                "v7 catalog batch count exceeds its commit timestamp range"
            );
            ensure!(
                self.batch_count != 1 || first == last,
                "v7 catalog singleton commit timestamps differ"
            );
        }
        ensure!(
            boundary.sealed_end == self.wal_bytes,
            "v7 catalog WAL length differs from its immutable boundary"
        );
        ensure!(
            boundary.wal_digest == self.wal_digest,
            "v7 catalog WAL digest differs from its immutable boundary"
        );
        ensure!(
            active.prefix_digest != [0; 32]
                && boundary.terminal_frontier_digest != [0; 32]
                && boundary.wal_digest != [0; 32],
            "v7 catalog boundary digest is empty"
        );
        if active.ticket_end == 0 {
            let header = WalV7Header {
                timeline_id: self.key.timeline_id,
                archive_epoch_id: self.key.archive_epoch_id,
                segment_id: self.key.segment_id,
                predecessor: self.predecessor,
                incarnation: active.incarnation,
            };
            ensure!(
                active.prefix_digest == header.digest()?,
                "empty v7 catalog prefix digest differs from its header"
            );
        }

        Ok(())
    }

    fn owned_bytes(&self) -> usize {
        self.immutable_boundary
            .as_ref()
            .map_or(0, |_| std::mem::size_of::<ImmutableBoundary>())
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
pub(crate) struct RetainedChainStart {
    pub(crate) timeline_id: TimelineId,
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) predecessor: ChainAnchor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RetentionSnapshot {
    pub(crate) repository_id: [u8; 16],
    pub(crate) replaced_prefix_high_water: u64,
    pub(crate) replaced_prefix_digest: [u8; 32],
    pub(crate) chain_starts: Vec<RetainedChainStart>,
    pub(crate) segments: Vec<SegmentMetadata>,
    pub(crate) breaks: Vec<CoverageBreak>,
    pub(crate) retention_cutoff: Option<RecordedAt>,
    pub(crate) oldest_advertised_commit_ts: Option<u64>,
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
    validate_replay(records, limits.max_decoded_state_bytes)?;
    for record in records {
        validate_record(record)?;
        if let PitrCatalogRecord::RetentionSnapshot(snapshot) = record {
            ensure!(
                snapshot.chain_starts.len() <= limits.max_snapshot_items,
                "snapshot chain-start limit exceeded"
            );
            ensure!(
                snapshot.segments.len() <= limits.max_snapshot_items,
                "snapshot segment limit exceeded"
            );
            ensure!(
                snapshot.breaks.len() <= limits.max_snapshot_items,
                "snapshot break limit exceeded"
            );
        }
    }
    let mut output = Vec::new();
    let base_sequence = match records.first() {
        Some(PitrCatalogRecord::RetentionSnapshot(snapshot)) => snapshot
            .replaced_prefix_high_water
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("catalog sequence exhausted"))?,
        _ => 1,
    };
    ensure!(
        base_sequence < u64::MAX,
        "replacement catalog cannot resume after snapshot"
    );

    for (index, record) in records.iter().enumerate() {
        let sequence = base_sequence
            .checked_add(
                u64::try_from(index).map_err(|_| anyhow::anyhow!("catalog sequence exhausted"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("catalog sequence exhausted"))?;
        ensure!(sequence < u64::MAX, "catalog sequence space exhausted");
        ensure!(
            !matches!(record, PitrCatalogRecord::RetentionSnapshot(_)) || index == 0,
            "retention snapshot must be the replacement catalog's first record"
        );
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
    let mut validator = ReplayValidator::default();
    let mut retained_owned_bytes = 0_usize;
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
        let header_crc = u32::from_be_bytes(input[offset + 18..offset + 22].try_into().unwrap());
        ensure!(
            crc32fast::hash(&input[offset + 4..offset + 18]) == header_crc,
            "PITR catalog frame header checksum mismatch"
        );
        let frame_len = FRAME_HEADER_BYTES
            .checked_add(payload_len)
            .and_then(|length| length.checked_add(FRAME_TRAILER_BYTES))
            .ok_or_else(|| anyhow::anyhow!("catalog frame length overflow"))?;
        if remaining < frame_len {
            break;
        }
        if offset != 0 {
            ensure!(
                sequence == expected_sequence,
                "catalog sequence is not contiguous"
            );
        }
        let payload_start = offset + FRAME_HEADER_BYTES;
        let payload_end = payload_start + payload_len;
        let payload = &input[payload_start..payload_end];
        let stored_crc =
            u32::from_be_bytes(input[payload_end..payload_end + 4].try_into().unwrap());
        ensure!(
            crc32fast::hash(&input[offset + 4..payload_end]) == stored_crc,
            "PITR catalog frame checksum mismatch"
        );
        let record = decode_record(
            payload,
            limits.max_snapshot_items,
            limits.max_decoded_state_bytes,
        )?;
        validate_record(&record)?;
        if offset == 0 {
            match &record {
                PitrCatalogRecord::RetentionSnapshot(snapshot) => {
                    let expected = snapshot
                        .replaced_prefix_high_water
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("catalog sequence exhausted"))?;
                    ensure!(
                        expected < u64::MAX,
                        "replacement catalog cannot resume after snapshot"
                    );
                    ensure!(sequence == expected, "retention snapshot sequence mismatch");
                    expected_sequence = expected;
                }
                _ => ensure!(sequence == 1, "first catalog record sequence must be one"),
            }
        } else {
            ensure!(
                !matches!(record, PitrCatalogRecord::RetentionSnapshot(_)),
                "retention snapshot must be the replacement catalog's first record"
            );
        }
        validator.apply(&record)?;
        retained_owned_bytes = retained_owned_bytes
            .checked_add(record_owned_bytes(&record))
            .ok_or_else(|| anyhow::anyhow!("catalog decoded-state size overflow"))?;
        let decoded_state_bytes =
            decoded_state_bytes(records.len() + 1, &validator, retained_owned_bytes)?;
        ensure!(
            decoded_state_bytes <= limits.max_decoded_state_bytes,
            "catalog decoded-state limit exceeded"
        );
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
    let header_crc = crc32fast::hash(&frame[4..]);
    frame.extend_from_slice(&header_crc.to_be_bytes());
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
            put_u64(&mut out, snapshot.replaced_prefix_high_water);
            put_bytes(&mut out, &snapshot.replaced_prefix_digest);
            put_u64(&mut out, snapshot.chain_starts.len() as u64);
            for start in &snapshot.chain_starts {
                put_bytes(&mut out, &start.timeline_id.0);
                put_bytes(&mut out, &start.archive_epoch_id.0);
                encode_chain_anchor(&mut out, &start.predecessor);
            }
            put_u64(&mut out, snapshot.segments.len() as u64);
            for metadata in &snapshot.segments {
                encode_metadata(&mut out, metadata)?;
            }
            put_u64(&mut out, snapshot.breaks.len() as u64);
            for break_record in &snapshot.breaks {
                encode_break(&mut out, break_record)?;
            }
            encode_optional_recorded_at(&mut out, snapshot.retention_cutoff);
            put_optional_u64(&mut out, snapshot.oldest_advertised_commit_ts);
            put_u64(&mut out, snapshot.backup_catalog_high_water);
            put_bytes(&mut out, &snapshot.backup_catalog_digest);
        }
    }
    Ok(out)
}

fn decode_record(
    input: &[u8],
    max_snapshot_items: usize,
    max_decoded_state_bytes: usize,
) -> Result<PitrCatalogRecord> {
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
            metadata: decode_metadata(
                record_body,
                max_decoded_state_bytes.saturating_sub(std::mem::size_of::<PitrCatalogRecord>()),
            )?,
        },
        2 => PitrCatalogRecord::CoverageBreak(decode_break(record_body)?),
        3 => {
            let mut cursor = Cursor::new(record_body);
            let repository_id = cursor.fixed::<16>()?;
            let replaced_prefix_high_water = cursor.u64()?;
            let replaced_prefix_digest = cursor.fixed::<32>()?;
            let mut snapshot_allocation = 0_usize;
            let chain_count = cursor.usize()?;
            ensure!(
                chain_count <= max_snapshot_items,
                "snapshot chain-start limit exceeded"
            );
            snapshot_allocation = snapshot_allocation
                .checked_add(
                    chain_count
                        .checked_mul(std::mem::size_of::<RetainedChainStart>())
                        .and_then(|bytes| bytes.checked_mul(5))
                        .ok_or_else(|| anyhow::anyhow!("snapshot allocation overflow"))?,
                )
                .ok_or_else(|| anyhow::anyhow!("snapshot allocation overflow"))?;
            ensure!(
                snapshot_allocation <= max_decoded_state_bytes,
                "snapshot allocation exceeds decoded-state limit"
            );
            let mut chain_starts = Vec::with_capacity(chain_count);
            for _ in 0..chain_count {
                chain_starts.push(RetainedChainStart {
                    timeline_id: TimelineId(cursor.fixed()?),
                    archive_epoch_id: ArchiveEpochId(cursor.fixed()?),
                    predecessor: decode_chain_anchor(&mut cursor)?,
                });
            }
            let segment_count = cursor.usize()?;
            ensure!(
                segment_count <= max_snapshot_items,
                "snapshot segment limit exceeded"
            );
            snapshot_allocation = snapshot_allocation
                .checked_add(
                    segment_count
                        .checked_mul(std::mem::size_of::<SegmentMetadata>())
                        .ok_or_else(|| anyhow::anyhow!("snapshot allocation overflow"))?,
                )
                .ok_or_else(|| anyhow::anyhow!("snapshot allocation overflow"))?;
            ensure!(
                snapshot_allocation <= max_decoded_state_bytes,
                "snapshot allocation exceeds decoded-state limit"
            );
            let mut segments = Vec::with_capacity(segment_count);
            for _ in 0..segment_count {
                let (metadata, used) = decode_metadata_prefix(
                    cursor.rest(),
                    max_decoded_state_bytes.saturating_sub(snapshot_allocation),
                )?;
                snapshot_allocation = snapshot_allocation
                    .checked_add(metadata.owned_bytes())
                    .ok_or_else(|| anyhow::anyhow!("snapshot allocation overflow"))?;
                cursor.advance(used)?;
                segments.push(metadata);
            }
            let break_count = cursor.usize()?;
            ensure!(
                break_count <= max_snapshot_items,
                "snapshot break limit exceeded"
            );
            snapshot_allocation = snapshot_allocation
                .checked_add(
                    break_count
                        .checked_mul(std::mem::size_of::<CoverageBreak>())
                        .ok_or_else(|| anyhow::anyhow!("snapshot allocation overflow"))?,
                )
                .ok_or_else(|| anyhow::anyhow!("snapshot allocation overflow"))?;
            ensure!(
                snapshot_allocation <= max_decoded_state_bytes,
                "snapshot allocation exceeds decoded-state limit"
            );
            let mut breaks = Vec::with_capacity(break_count);
            for _ in 0..break_count {
                let (break_record, used) = decode_break_prefix(cursor.rest())?;
                cursor.advance(used)?;
                breaks.push(break_record);
            }
            let retention_cutoff = decode_optional_recorded_at(&mut cursor)?;
            let oldest_advertised_commit_ts = cursor.optional_u64()?;
            let backup_catalog_high_water = cursor.u64()?;
            let backup_catalog_digest = cursor.fixed()?;
            cursor.finish()?;
            PitrCatalogRecord::RetentionSnapshot(RetentionSnapshot {
                repository_id,
                replaced_prefix_high_water,
                replaced_prefix_digest,
                chain_starts,
                segments,
                breaks,
                retention_cutoff,
                oldest_advertised_commit_ts,
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
                (snapshot.replaced_prefix_high_water == 0)
                    == (snapshot.replaced_prefix_digest == [0; 32]),
                "snapshot replaced-prefix binding is incomplete"
            );
            ensure!(
                (snapshot.backup_catalog_high_water == 0)
                    == (snapshot.backup_catalog_digest == [0; 32]),
                "snapshot backup binding is incomplete"
            );
            ensure!(
                snapshot.chain_starts.windows(2).all(|pair| compare_chain(
                    pair[0].timeline_id,
                    pair[0].archive_epoch_id,
                    pair[1].timeline_id,
                    pair[1].archive_epoch_id,
                ) == Ordering::Less),
                "snapshot chain starts are not canonically ordered"
            );
            ensure!(
                snapshot
                    .chain_starts
                    .iter()
                    .all(|start| matches!(start.predecessor, ChainAnchor::Segment(_))),
                "snapshot chain starts must name an external segment"
            );
            ensure!(
                snapshot
                    .segments
                    .windows(2)
                    .all(|pair| compare_segment_key(&pair[0].key, &pair[1].key) == Ordering::Less),
                "snapshot segments are not ordered"
            );
            ensure!(
                snapshot.breaks.windows(2).all(|pair| compare_chain(
                    pair[0].timeline_id,
                    pair[0].archive_epoch_id,
                    pair[1].timeline_id,
                    pair[1].archive_epoch_id
                ) == Ordering::Less),
                "snapshot coverage breaks are not canonically ordered"
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

fn compare_chain(
    left_timeline: TimelineId,
    left_epoch: ArchiveEpochId,
    right_timeline: TimelineId,
    right_epoch: ArchiveEpochId,
) -> Ordering {
    left_timeline
        .0
        .cmp(&right_timeline.0)
        .then_with(|| left_epoch.0.cmp(&right_epoch.0))
}

fn compare_segment_key(left: &SegmentKey, right: &SegmentKey) -> Ordering {
    compare_chain(
        left.timeline_id,
        left.archive_epoch_id,
        right.timeline_id,
        right.archive_epoch_id,
    )
    .then_with(|| left.segment_id.cmp(&right.segment_id))
}

fn validate_break(break_record: &CoverageBreak) -> Result<()> {
    // A break with no uncovered commit is only valid before the epoch has
    // committed anything, so it must not carry a timestamp either way.
    if let Some(first_uncovered) = break_record.first_uncovered_commit_ts {
        ensure!(
            first_uncovered != 0,
            "coverage break names a zero uncovered commit"
        );
    }

    Ok(())
}

fn validate_replay(records: &[PitrCatalogRecord], max_decoded_state_bytes: usize) -> Result<()> {
    let mut validator = ReplayValidator::default();
    let mut retained_owned_bytes = 0_usize;

    for (index, record) in records.iter().enumerate() {
        validator.apply(record)?;
        retained_owned_bytes = retained_owned_bytes
            .checked_add(record_owned_bytes(record))
            .ok_or_else(|| anyhow::anyhow!("catalog decoded-state size overflow"))?;
        ensure!(
            decoded_state_bytes(index + 1, &validator, retained_owned_bytes)?
                <= max_decoded_state_bytes,
            "catalog decoded-state limit exceeded"
        );
    }
    Ok(())
}

fn decoded_state_bytes(
    record_count: usize,
    validator: &ReplayValidator,
    retained_owned_bytes: usize,
) -> Result<usize> {
    const HASH_ENTRY_OVERHEAD_FACTOR: usize = 4;
    let record_bytes = record_count
        .checked_mul(std::mem::size_of::<PitrCatalogRecord>())
        .ok_or_else(|| anyhow::anyhow!("catalog decoded-state size overflow"))?;
    let segment_bytes = validator
        .seen_segments
        .len()
        .checked_mul(std::mem::size_of::<SegmentKey>())
        .and_then(|bytes| bytes.checked_mul(HASH_ENTRY_OVERHEAD_FACTOR))
        .ok_or_else(|| anyhow::anyhow!("catalog decoded-state size overflow"))?;
    let broken_bytes = validator
        .broken
        .len()
        .checked_mul(std::mem::size_of::<(TimelineId, ArchiveEpochId)>())
        .and_then(|bytes| bytes.checked_mul(HASH_ENTRY_OVERHEAD_FACTOR))
        .ok_or_else(|| anyhow::anyhow!("catalog decoded-state size overflow"))?;
    let chain_bytes = validator
        .chain_heads
        .len()
        .checked_mul(std::mem::size_of::<(
            (TimelineId, ArchiveEpochId),
            (SegmentAnchor, Option<u64>),
        )>())
        .and_then(|bytes| bytes.checked_mul(HASH_ENTRY_OVERHEAD_FACTOR))
        .ok_or_else(|| anyhow::anyhow!("catalog decoded-state size overflow"))?;
    let chain_start_bytes = validator
        .chain_starts
        .len()
        .checked_mul(std::mem::size_of::<(
            (TimelineId, ArchiveEpochId),
            ChainAnchor,
        )>())
        .and_then(|bytes| bytes.checked_mul(HASH_ENTRY_OVERHEAD_FACTOR))
        .ok_or_else(|| anyhow::anyhow!("catalog decoded-state size overflow"))?;

    record_bytes
        .checked_add(retained_owned_bytes)
        .and_then(|bytes| bytes.checked_add(segment_bytes))
        .and_then(|bytes| bytes.checked_add(broken_bytes))
        .and_then(|bytes| bytes.checked_add(chain_bytes))
        .and_then(|bytes| bytes.checked_add(chain_start_bytes))
        .ok_or_else(|| anyhow::anyhow!("catalog decoded-state size overflow"))
}

fn record_owned_bytes(record: &PitrCatalogRecord) -> usize {
    match record {
        PitrCatalogRecord::CommitSegment { metadata } => metadata.owned_bytes(),
        PitrCatalogRecord::CoverageBreak(_) => 0,
        PitrCatalogRecord::RetentionSnapshot(snapshot) => snapshot
            .chain_starts
            .len()
            .saturating_mul(std::mem::size_of::<RetainedChainStart>())
            .saturating_mul(5)
            .saturating_add(
                snapshot
                    .segments
                    .len()
                    .saturating_mul(std::mem::size_of::<SegmentMetadata>()),
            )
            .saturating_add(
                snapshot
                    .breaks
                    .len()
                    .saturating_mul(std::mem::size_of::<CoverageBreak>()),
            )
            .saturating_add(snapshot.segments.iter().fold(0_usize, |bytes, metadata| {
                bytes.saturating_add(metadata.owned_bytes())
            })),
    }
}

#[derive(Default)]
struct ReplayValidator {
    repository_id: Option<[u8; 16]>,
    seen_segments: HashSet<SegmentKey>,
    broken: HashSet<(TimelineId, ArchiveEpochId)>,
    chain_starts: HashMap<(TimelineId, ArchiveEpochId), ChainAnchor>,
    chain_heads: HashMap<(TimelineId, ArchiveEpochId), (SegmentAnchor, Option<u64>)>,
}

impl ReplayValidator {
    fn apply(&mut self, record: &PitrCatalogRecord) -> Result<()> {
        match record {
            PitrCatalogRecord::CommitSegment { metadata } => {
                self.validate_repository(metadata.key.repository_id)?;
                let chain = (metadata.key.timeline_id, metadata.key.archive_epoch_id);
                ensure!(
                    !self.broken.contains(&chain),
                    "segment committed after coverage break"
                );
                if let Some((previous, previous_last)) = self.chain_heads.get(&chain) {
                    ensure!(
                        metadata.predecessor == ChainAnchor::Segment(*previous),
                        "segment predecessor chain mismatch"
                    );
                    if let (Some(previous_last), Some(first)) =
                        (*previous_last, metadata.first_commit_ts)
                    {
                        ensure!(
                            first > previous_last,
                            "segment commit timestamps overlap or regress"
                        );
                    }
                } else {
                    ensure!(
                        metadata.predecessor
                            == ChainAnchor::Genesis {
                                archive_epoch_id: metadata.key.archive_epoch_id
                            },
                        "first segment must use epoch genesis"
                    );
                }
                ensure!(
                    self.seen_segments.insert(metadata.key),
                    "segment identity was already committed"
                );
                let last_commit = metadata
                    .last_commit_ts
                    .or_else(|| self.chain_heads.get(&chain).and_then(|(_, last)| *last));
                self.chain_heads
                    .insert(chain, (metadata.anchor, last_commit));
            }
            PitrCatalogRecord::CoverageBreak(break_record) => {
                self.validate_repository(break_record.repository_id)?;
                let chain = (break_record.timeline_id, break_record.archive_epoch_id);
                ensure!(
                    self.broken.insert(chain),
                    "coverage break was already recorded"
                );
                if let Some((previous, previous_last)) = self.chain_heads.get(&chain) {
                    ensure!(
                        break_record.after == ChainAnchor::Segment(*previous),
                        "coverage break predecessor mismatch"
                    );
                    if let (Some(previous_last), Some(first)) =
                        (*previous_last, break_record.first_uncovered_commit_ts)
                    {
                        ensure!(first > previous_last, "coverage break timestamp regressed");
                    }
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
                self.validate_repository(snapshot.repository_id)?;
                self.seen_segments.clear();
                self.chain_starts.clear();
                self.chain_heads.clear();
                self.broken.clear();
                for start in &snapshot.chain_starts {
                    let chain = (start.timeline_id, start.archive_epoch_id);
                    ensure!(
                        self.chain_starts.insert(chain, start.predecessor).is_none(),
                        "duplicate snapshot chain start"
                    );
                }
                for metadata in &snapshot.segments {
                    self.validate_repository(metadata.key.repository_id)?;
                    let chain = (metadata.key.timeline_id, metadata.key.archive_epoch_id);
                    if let Some((previous, previous_last)) = self.chain_heads.get(&chain) {
                        ensure!(
                            metadata.predecessor == ChainAnchor::Segment(*previous),
                            "snapshot segment predecessor mismatch"
                        );
                        if let (Some(previous_last), Some(first)) =
                            (*previous_last, metadata.first_commit_ts)
                        {
                            ensure!(
                                first > previous_last,
                                "snapshot segment timestamps overlap or regress"
                            );
                        }
                    } else {
                        let expected = if let Some(expected) = self.chain_starts.remove(&chain) {
                            expected
                        } else {
                            ChainAnchor::Genesis {
                                archive_epoch_id: metadata.key.archive_epoch_id,
                            }
                        };
                        ensure!(
                            metadata.predecessor == expected,
                            "snapshot segment has no matching retained chain start"
                        );
                    }
                    ensure!(
                        self.seen_segments.insert(metadata.key),
                        "snapshot segment identity was already committed"
                    );
                    let last_commit = metadata
                        .last_commit_ts
                        .or_else(|| self.chain_heads.get(&chain).and_then(|(_, last)| *last));
                    self.chain_heads
                        .insert(chain, (metadata.anchor, last_commit));
                }
                for break_record in &snapshot.breaks {
                    self.validate_repository(break_record.repository_id)?;
                    let chain = (break_record.timeline_id, break_record.archive_epoch_id);
                    ensure!(
                        self.broken.insert(chain),
                        "duplicate snapshot coverage break"
                    );
                    if let Some((previous, previous_last)) = self.chain_heads.get(&chain) {
                        ensure!(
                            break_record.after == ChainAnchor::Segment(*previous),
                            "snapshot coverage break predecessor mismatch"
                        );
                        if let (Some(previous_last), Some(first)) =
                            (*previous_last, break_record.first_uncovered_commit_ts)
                        {
                            ensure!(
                                first > previous_last,
                                "snapshot coverage break timestamp regressed"
                            );
                        }
                    } else {
                        let expected = if let Some(expected) = self.chain_starts.remove(&chain) {
                            expected
                        } else {
                            ChainAnchor::Genesis {
                                archive_epoch_id: break_record.archive_epoch_id,
                            }
                        };
                        ensure!(
                            break_record.after == expected,
                            "snapshot coverage break has no retained predecessor"
                        );
                    }
                }
                ensure!(
                    self.chain_starts.is_empty(),
                    "snapshot contains an unused chain start"
                );
            }
        }
        Ok(())
    }

    fn validate_repository(&mut self, repository_id: [u8; 16]) -> Result<()> {
        if let Some(expected) = self.repository_id {
            ensure!(
                expected == repository_id,
                "catalog repository identity changed"
            );
        } else {
            self.repository_id = Some(repository_id);
        }
        Ok(())
    }
}

fn encode_metadata(out: &mut Vec<u8>, metadata: &SegmentMetadata) -> Result<()> {
    metadata.validate()?;
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
    if let Some(boundary) = metadata.immutable_boundary.as_deref() {
        encode_immutable_boundary(out, *boundary);
    }

    Ok(())
}

fn decode_metadata(input: &[u8], max_owned_bytes: usize) -> Result<SegmentMetadata> {
    let (metadata, used) = decode_metadata_prefix(input, max_owned_bytes)?;
    ensure!(used == input.len(), "trailing bytes in segment metadata");

    Ok(metadata)
}

fn decode_metadata_prefix(
    input: &[u8],
    max_owned_bytes: usize,
) -> Result<(SegmentMetadata, usize)> {
    let mut cursor = Cursor::new(input);
    let key = SegmentKey {
        repository_id: cursor.fixed()?,
        timeline_id: TimelineId(cursor.fixed()?),
        archive_epoch_id: ArchiveEpochId(cursor.fixed()?),
        segment_id: SegmentId(cursor.u64()?),
    };
    let wal_format_version = cursor.u16()?;
    let seal_format_version = cursor.u16()?;
    let anchor = decode_anchor(&mut cursor)?;
    let predecessor = decode_chain_anchor(&mut cursor)?;
    let first_commit_ts = cursor.optional_u64()?;
    let last_commit_ts = cursor.optional_u64()?;
    let batch_count = cursor.u64()?;
    let logical_bytes = cursor.u64()?;
    let wal_bytes = cursor.u64()?;
    let wal_digest = cursor.fixed()?;
    let seal_digest = cursor.fixed()?;
    let source_identity = cursor.fixed()?;
    let immutable_boundary = match seal_format_version {
        1 => None,
        2 if wal_format_version == crate::wal::format::WAL_FORMAT_VERSION_V7 => {
            ensure!(
                std::mem::size_of::<ImmutableBoundary>() <= max_owned_bytes,
                "catalog metadata allocation exceeds decoded-state limit"
            );
            Some(Box::new(decode_immutable_boundary(&mut cursor)?))
        }
        _ => bail!("unsupported PITR WAL/seal descriptor"),
    };
    let metadata = SegmentMetadata {
        key,
        wal_format_version,
        seal_format_version,
        immutable_boundary,
        anchor,
        predecessor,
        first_commit_ts,
        last_commit_ts,
        batch_count,
        logical_bytes,
        wal_bytes,
        wal_digest,
        seal_digest,
        source_identity,
    };
    metadata.validate()?;

    Ok((metadata, cursor.position()))
}

fn encode_immutable_boundary(out: &mut Vec<u8>, boundary: ImmutableBoundary) {
    let active = boundary.active;
    put_bytes(out, &active.timeline_id);
    put_bytes(out, &active.archive_epoch_id);
    put_u64(out, active.segment_id);
    put_bytes(out, &active.incarnation);
    put_u64(out, active.ticket_end);
    put_u64(out, active.durable_end);
    put_u64(out, active.last_commit_ts);
    put_bytes(out, &active.prefix_digest);
    put_u64(out, boundary.sealed_end);
    put_bytes(out, &boundary.terminal_frontier_digest);
    put_bytes(out, &boundary.wal_digest);
}

fn decode_immutable_boundary(cursor: &mut Cursor<'_>) -> Result<ImmutableBoundary> {
    let active = crate::pitr::manifest::ActiveBoundary {
        timeline_id: cursor.fixed()?,
        archive_epoch_id: cursor.fixed()?,
        segment_id: cursor.u64()?,
        incarnation: cursor.fixed()?,
        ticket_end: cursor.u64()?,
        durable_end: cursor.u64()?,
        last_commit_ts: cursor.u64()?,
        prefix_digest: cursor.fixed()?,
    };
    let boundary = ImmutableBoundary {
        active,
        sealed_end: cursor.u64()?,
        terminal_frontier_digest: cursor.fixed()?,
        wal_digest: cursor.fixed()?,
    };
    boundary.validate()?;
    Ok(boundary)
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
fn encode_optional_recorded_at(out: &mut Vec<u8>, value: Option<RecordedAt>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.secs.to_be_bytes());
            out.extend_from_slice(&value.nanos.to_be_bytes());
        }
        None => out.push(0),
    }
}

fn decode_optional_recorded_at(cursor: &mut Cursor<'_>) -> Result<Option<RecordedAt>> {
    match cursor.u8()? {
        0 => Ok(None),
        1 => {
            let secs = i64::from_be_bytes(cursor.fixed()?);
            let nanos = u32::from_be_bytes(cursor.fixed()?);
            ensure!(nanos < 1_000_000_000, "recorded_at nanos out of range");

            Ok(Some(RecordedAt { secs, nanos }))
        }
        _ => bail!("invalid optional recorded_at"),
    }
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
            wal_format_version: crate::pitr::WAL_V5_VERSION,
            seal_format_version: 1,
            immutable_boundary: None,
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

    fn v7_metadata(segment_id: u64, predecessor: ChainAnchor) -> SegmentMetadata {
        let wal_digest = [segment_id as u8 + 20; 32];
        let seal_digest = [segment_id as u8 + 30; 32];
        let key = SegmentKey {
            repository_id: [7; 16],
            timeline_id: TimelineId([8; 16]),
            archive_epoch_id: ArchiveEpochId([9; 16]),
            segment_id: SegmentId(segment_id),
        };
        let boundary = ImmutableBoundary {
            active: crate::pitr::manifest::ActiveBoundary {
                timeline_id: key.timeline_id.0,
                archive_epoch_id: key.archive_epoch_id.0,
                segment_id,
                incarnation: [11; 16],
                ticket_end: 1,
                durable_end: 3 * 4096,
                last_commit_ts: segment_id * 10,
                prefix_digest: [12; 32],
            },
            sealed_end: 4 * 4096,
            terminal_frontier_digest: [13; 32],
            wal_digest,
        };

        SegmentMetadata {
            key,
            wal_format_version: crate::wal::format::WAL_FORMAT_VERSION_V7,
            seal_format_version: 2,
            immutable_boundary: Some(Box::new(boundary)),
            anchor: SegmentAnchor {
                segment_id: key.segment_id,
                wal_digest,
                seal_digest,
            },
            predecessor,
            first_commit_ts: Some(segment_id * 10),
            last_commit_ts: Some(segment_id * 10),
            batch_count: 1,
            logical_bytes: 100,
            wal_bytes: boundary.sealed_end,
            wal_digest,
            seal_digest,
            source_identity: [segment_id as u8 + 40; 32],
        }
    }

    fn empty_v7_metadata(segment_id: u64, predecessor: ChainAnchor) -> SegmentMetadata {
        use crate::wal::v7::codec::{
            WAL_V7_HEADER_LEN, WalV7Header, encode_generation_zero_frontier,
        };

        let mut metadata = v7_metadata(segment_id, predecessor);
        let header = WalV7Header {
            timeline_id: metadata.key.timeline_id,
            archive_epoch_id: metadata.key.archive_epoch_id,
            segment_id: metadata.key.segment_id,
            predecessor,
            incarnation: metadata
                .immutable_boundary
                .as_ref()
                .unwrap()
                .active
                .incarnation,
        };
        let header_digest = header.digest().unwrap();
        let mut wal = header.encode().unwrap().to_vec();
        wal.extend_from_slice(&encode_generation_zero_frontier(header_digest).unwrap());
        let active = crate::pitr::manifest::ActiveBoundary {
            timeline_id: header.timeline_id.0,
            archive_epoch_id: header.archive_epoch_id.0,
            segment_id,
            incarnation: header.incarnation,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
        };
        let (seal, seal_bytes) =
            crate::pitr::seal::build_v7_seal(&wal, active, predecessor).unwrap();
        metadata.first_commit_ts = None;
        metadata.last_commit_ts = None;
        metadata.batch_count = 0;
        metadata.logical_bytes = 0;
        metadata.wal_bytes = seal.boundary.sealed_end;
        metadata.wal_digest = seal.boundary.wal_digest;
        metadata.seal_digest = Sha256::digest(seal_bytes).into();
        metadata.anchor.wal_digest = metadata.wal_digest;
        metadata.anchor.seal_digest = metadata.seal_digest;
        metadata.immutable_boundary = Some(Box::new(seal.boundary));

        metadata
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
    fn catalog_round_trips_v7_immutable_segment_boundary() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let v7 = v7_metadata(2, ChainAnchor::Segment(first.anchor));
        v7.validate().unwrap();
        let records = vec![
            PitrCatalogRecord::CommitSegment { metadata: first },
            PitrCatalogRecord::CommitSegment {
                metadata: v7.clone(),
            },
        ];

        let encoded = encode_catalog(&records).unwrap();
        let replay = replay_catalog(&encoded).unwrap();

        assert_eq!(replay.records, records);
        assert_eq!(replay.retained_offset, encoded.len());

        let snapshot_first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let snapshot_v7 = v7_metadata(2, ChainAnchor::Segment(snapshot_first.anchor));
        let snapshot = RetentionSnapshot {
            repository_id: [7; 16],
            replaced_prefix_high_water: 0,
            replaced_prefix_digest: [0; 32],
            chain_starts: vec![],
            segments: vec![snapshot_first, snapshot_v7],
            breaks: vec![],
            retention_cutoff: None,
            oldest_advertised_commit_ts: None,
            backup_catalog_high_water: 0,
            backup_catalog_digest: [0; 32],
        };
        let snapshot_record = PitrCatalogRecord::RetentionSnapshot(snapshot);
        let encoded_snapshot = encode_catalog(std::slice::from_ref(&snapshot_record)).unwrap();
        let replayed_snapshot = replay_catalog(&encoded_snapshot).unwrap();
        assert_eq!(
            replayed_snapshot.records,
            vec![snapshot_record],
            "snapshot metadata prefixes must consume the v7 descriptor"
        );
    }

    #[test]
    fn v7_catalog_descriptor_requires_a_matching_immutable_boundary() {
        let mut missing = v7_metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        missing.immutable_boundary = None;
        assert!(missing.validate().is_err());

        let mut mismatched = v7_metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        mismatched.immutable_boundary.as_mut().unwrap().wal_digest = [99; 32];
        assert!(mismatched.validate().is_err());
    }

    fn refresh_catalog_frame_checksums(frame: &mut [u8]) {
        let digest_end = frame.len() - FRAME_TRAILER_BYTES;
        let digest_start = digest_end - RECORD_DIGEST_BYTES;
        let digest: [u8; 32] = Sha256::digest(&frame[FRAME_HEADER_BYTES..digest_start]).into();
        frame[digest_start..digest_end].copy_from_slice(&digest);
        let crc = crc32fast::hash(&frame[4..digest_end]);
        frame[digest_end..].copy_from_slice(&crc.to_be_bytes());
    }

    fn assert_v7_descriptor_rejected(metadata: SegmentMetadata) {
        assert!(metadata.validate().is_err());
        assert!(
            encode_catalog(&[PitrCatalogRecord::CommitSegment {
                metadata: metadata.clone(),
            }])
            .is_err()
        );

        // Construct a checksum-valid catalog record independently of v7
        // validation, using the unchanged legacy encoding for its common fields.
        let mut legacy = metadata.clone();
        legacy.wal_format_version = crate::pitr::WAL_V5_VERSION;
        legacy.seal_format_version = 1;
        legacy.immutable_boundary = None;
        let mut wire_metadata = Vec::new();
        encode_metadata(&mut wire_metadata, &legacy).unwrap();
        let version_offset = 3 * 16 + 8;
        wire_metadata[version_offset..version_offset + 2]
            .copy_from_slice(&metadata.wal_format_version.to_be_bytes());
        wire_metadata[version_offset + 2..version_offset + 4]
            .copy_from_slice(&metadata.seal_format_version.to_be_bytes());
        encode_immutable_boundary(&mut wire_metadata, *metadata.immutable_boundary.unwrap());

        let baseline = if metadata.batch_count == 0 {
            empty_v7_metadata(1, metadata.predecessor)
        } else {
            v7_metadata(1, metadata.predecessor)
        };
        let mut frame =
            encode_catalog(&[PitrCatalogRecord::CommitSegment { metadata: baseline }]).unwrap();
        let digest_end = frame.len() - FRAME_TRAILER_BYTES;
        let digest_start = digest_end - RECORD_DIGEST_BYTES;
        let metadata_start = FRAME_HEADER_BYTES + 1;
        assert_eq!(wire_metadata.len(), digest_start - metadata_start);
        frame[metadata_start..digest_start].copy_from_slice(&wire_metadata);
        refresh_catalog_frame_checksums(&mut frame);
        assert!(replay_catalog(&frame).is_err());
    }

    #[test]
    fn v7_catalog_rejects_impossible_frozen_image_descriptors() {
        let valid = v7_metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let mut terminal_gap = valid.clone();
        terminal_gap.immutable_boundary.as_mut().unwrap().sealed_end += 4096;
        terminal_gap.wal_bytes += 4096;
        assert_v7_descriptor_rejected(terminal_gap);

        let mut oversized = valid.clone();
        let boundary = oversized.immutable_boundary.as_mut().unwrap();
        boundary.active.durable_end = crate::wal::MAX_WAL_FILE_SIZE;
        boundary.sealed_end = crate::wal::MAX_WAL_FILE_SIZE + 4096;
        oversized.wal_bytes = boundary.sealed_end;
        assert_v7_descriptor_rejected(oversized);

        let mut too_many_tickets = valid.clone();
        too_many_tickets.batch_count = 2;
        too_many_tickets
            .immutable_boundary
            .as_mut()
            .unwrap()
            .active
            .ticket_end = 2;
        assert_v7_descriptor_rejected(too_many_tickets);

        let mut zero_first_commit = valid.clone();
        zero_first_commit.first_commit_ts = Some(0);
        assert_v7_descriptor_rejected(zero_first_commit);

        let mut empty_prefix_digest = valid.clone();
        empty_prefix_digest
            .immutable_boundary
            .as_mut()
            .unwrap()
            .active
            .prefix_digest = [0; 32];
        assert_v7_descriptor_rejected(empty_prefix_digest);

        let mut empty_terminal_digest = valid.clone();
        empty_terminal_digest
            .immutable_boundary
            .as_mut()
            .unwrap()
            .terminal_frontier_digest = [0; 32];
        assert_v7_descriptor_rejected(empty_terminal_digest);

        let mut empty_image_digest = valid;
        empty_image_digest
            .immutable_boundary
            .as_mut()
            .unwrap()
            .wal_digest = [0; 32];
        empty_image_digest.wal_digest = [0; 32];
        empty_image_digest.anchor.wal_digest = [0; 32];
        assert_v7_descriptor_rejected(empty_image_digest);
    }

    #[test]
    fn catalog_round_trips_empty_v7_segment() {
        let empty = empty_v7_metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let records = [PitrCatalogRecord::CommitSegment { metadata: empty }];
        let encoded = encode_catalog(&records).unwrap();
        assert_eq!(replay_catalog(&encoded).unwrap().records, records);
    }

    #[test]
    fn v7_catalog_rejects_timestamp_ranges_that_cannot_match_its_batches() {
        let valid = v7_metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let mut unequal_singleton = valid.clone();
        unequal_singleton.first_commit_ts = Some(9);
        assert_v7_descriptor_rejected(unequal_singleton);

        let mut multiple = valid.clone();
        multiple.batch_count = 2;
        let boundary = multiple.immutable_boundary.as_mut().unwrap();
        boundary.active.ticket_end = 2;
        boundary.active.durable_end = 4 * 4096;
        boundary.sealed_end = 5 * 4096;
        multiple.wal_bytes = boundary.sealed_end;
        assert_v7_descriptor_rejected(multiple.clone());

        multiple.first_commit_ts = Some(9);
        let mut crowded_range = multiple.clone();
        crowded_range.batch_count = 3;
        let boundary = crowded_range.immutable_boundary.as_mut().unwrap();
        boundary.active.ticket_end = 3;
        boundary.active.durable_end = 5 * 4096;
        boundary.sealed_end = 6 * 4096;
        crowded_range.wal_bytes = boundary.sealed_end;
        assert_v7_descriptor_rejected(crowded_range);

        // Gaps are valid, and timestamp capacity remains representable when the
        // final commit is u64::MAX.
        for (mut segment, first, last) in [
            (multiple.clone(), 1, 10),
            (multiple, u64::MAX - 1, u64::MAX),
            (valid, u64::MAX, u64::MAX),
        ] {
            segment.first_commit_ts = Some(first);
            segment.last_commit_ts = Some(last);
            segment
                .immutable_boundary
                .as_mut()
                .unwrap()
                .active
                .last_commit_ts = last;
            let records = [PitrCatalogRecord::CommitSegment { metadata: segment }];
            let encoded = encode_catalog(&records).unwrap();
            assert_eq!(replay_catalog(&encoded).unwrap().records, records);
        }
    }

    #[test]
    fn v7_catalog_rejects_an_empty_prefix_digest_that_does_not_bind_its_header() {
        let mut empty = empty_v7_metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        empty
            .immutable_boundary
            .as_mut()
            .unwrap()
            .active
            .prefix_digest[0] ^= 1;
        assert_v7_descriptor_rejected(empty);
    }

    #[test]
    fn v7_snapshot_rejects_empty_predecessor_digests_even_when_chain_start_matches() {
        let predecessor = SegmentAnchor {
            segment_id: SegmentId(1),
            wal_digest: [1; 32],
            seal_digest: [2; 32],
        };
        let segment = v7_metadata(2, ChainAnchor::Segment(predecessor));
        let snapshot = RetentionSnapshot {
            repository_id: segment.key.repository_id,
            replaced_prefix_high_water: 0,
            replaced_prefix_digest: [0; 32],
            chain_starts: vec![RetainedChainStart {
                timeline_id: segment.key.timeline_id,
                archive_epoch_id: segment.key.archive_epoch_id,
                predecessor: segment.predecessor,
            }],
            segments: vec![segment],
            breaks: vec![],
            retention_cutoff: None,
            oldest_advertised_commit_ts: None,
            backup_catalog_high_water: 0,
            backup_catalog_digest: [0; 32],
        };
        let records = [PitrCatalogRecord::RetentionSnapshot(snapshot.clone())];
        let encoded = encode_catalog(&records).unwrap();
        assert_eq!(replay_catalog(&encoded).unwrap().records, records);

        // Locate both copies of the predecessor in the unchanged snapshot wire
        // layout. Mutate them together so chain equality still holds.
        let snapshot_offset = FRAME_HEADER_BYTES + 1;
        let mut cursor = Cursor::new(&encoded[snapshot_offset..]);
        cursor.advance(16 + 8 + 32).unwrap();
        assert_eq!(cursor.u64().unwrap(), 1);
        cursor.advance(16 + 16 + 1 + 8).unwrap();
        let chain_digest_offset = snapshot_offset + cursor.position();
        cursor.advance(2 * 32).unwrap();
        assert_eq!(cursor.u64().unwrap(), 1);
        cursor
            .advance(3 * 16 + 8 + 2 + 2 + 8 + 32 + 32 + 1 + 8)
            .unwrap();
        let segment_digest_offset = snapshot_offset + cursor.position();
        for offset in [chain_digest_offset, segment_digest_offset] {
            assert_eq!(&encoded[offset..offset + 32], &predecessor.wal_digest);
            assert_eq!(&encoded[offset + 32..offset + 64], &predecessor.seal_digest);
        }

        for digest_index in 0..2 {
            let mut invalid_predecessor = predecessor;
            if digest_index == 0 {
                invalid_predecessor.wal_digest = [0; 32];
            } else {
                invalid_predecessor.seal_digest = [0; 32];
            }
            let mut invalid = snapshot.clone();
            invalid.chain_starts[0].predecessor = ChainAnchor::Segment(invalid_predecessor);
            invalid.segments[0].predecessor = ChainAnchor::Segment(invalid_predecessor);
            assert!(invalid.segments[0].validate().is_err());
            assert!(
                encode_catalog(&[PitrCatalogRecord::RetentionSnapshot(invalid.clone())]).is_err()
            );

            let mut corrupt = encoded.clone();
            for offset in [chain_digest_offset, segment_digest_offset] {
                let start = offset + digest_index * 32;
                corrupt[start..start + 32].fill(0);
            }
            refresh_catalog_frame_checksums(&mut corrupt);
            let error = replay_catalog(&corrupt).unwrap_err().to_string();
            assert!(error.contains("v7 catalog predecessor digest is empty"));

            // Preserve the existing v5/v6 descriptor validation rules.
            invalid.segments[0].wal_format_version = crate::pitr::WAL_V5_VERSION;
            invalid.segments[0].seal_format_version = 1;
            invalid.segments[0].immutable_boundary = None;
            let legacy = [PitrCatalogRecord::RetentionSnapshot(invalid)];
            let legacy_encoded = encode_catalog(&legacy).unwrap();
            assert_eq!(replay_catalog(&legacy_encoded).unwrap().records, legacy);
        }
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
        let mut corrupt = encoded.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(replay_catalog(&corrupt).is_err());
        let mut corrupt_length = encoded;
        corrupt_length[6] ^= 1;
        assert!(replay_catalog(&corrupt_length).is_err());
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
            encode_catalog(&[
                PitrCatalogRecord::CommitSegment {
                    metadata: first.clone()
                },
                duplicate
            ])
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
            encode_catalog(&[
                PitrCatalogRecord::CommitSegment { metadata: first },
                break_record,
                PitrCatalogRecord::CommitSegment { metadata: later }
            ])
            .is_err()
        );
    }

    #[test]
    fn replay_rejects_timestamp_overlap() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let mut overlapping = metadata(2, ChainAnchor::Segment(first.anchor));
        overlapping.first_commit_ts = Some(19);
        assert!(
            encode_catalog(&[
                PitrCatalogRecord::CommitSegment {
                    metadata: first.clone()
                },
                PitrCatalogRecord::CommitSegment {
                    metadata: overlapping
                },
            ])
            .is_err()
        );
    }

    #[test]
    fn replay_preserves_high_water_across_empty_segments() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let mut empty = metadata(2, ChainAnchor::Segment(first.anchor));
        empty.first_commit_ts = None;
        empty.last_commit_ts = None;
        empty.batch_count = 0;
        let mut regressing = metadata(3, ChainAnchor::Segment(empty.anchor));
        regressing.first_commit_ts = Some(19);
        regressing.last_commit_ts = Some(25);
        assert!(
            encode_catalog(&[
                PitrCatalogRecord::CommitSegment {
                    metadata: first.clone()
                },
                PitrCatalogRecord::CommitSegment { metadata: empty },
                PitrCatalogRecord::CommitSegment {
                    metadata: regressing
                },
            ])
            .is_err()
        );
    }

    #[test]
    fn encoder_and_replay_enforce_the_same_decoded_state_limit() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let second = metadata(2, ChainAnchor::Segment(first.anchor));
        let records = [
            PitrCatalogRecord::CommitSegment { metadata: first },
            PitrCatalogRecord::CommitSegment { metadata: second },
        ];
        let encoded = encode_catalog(&records).unwrap();
        let limits = PitrCatalogLimits {
            max_decoded_state_bytes: 1,
            ..PitrCatalogLimits::default()
        };
        assert!(encode_catalog_with_limits(&records, limits).is_err());
        assert!(replay_catalog_with_limits(&encoded, limits).is_err());
    }

    #[test]
    fn v7_boundary_allocations_count_toward_catalog_and_snapshot_limits() {
        let segment = v7_metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let snapshot = RetentionSnapshot {
            repository_id: segment.key.repository_id,
            replaced_prefix_high_water: 0,
            replaced_prefix_digest: [0; 32],
            chain_starts: vec![],
            segments: vec![segment.clone()],
            breaks: vec![],
            retention_cutoff: None,
            oldest_advertised_commit_ts: None,
            backup_catalog_high_water: 0,
            backup_catalog_digest: [0; 32],
        };
        for (record, common_owned_bytes, decoder_base_bytes) in [
            (
                PitrCatalogRecord::CommitSegment { metadata: segment },
                0,
                std::mem::size_of::<PitrCatalogRecord>(),
            ),
            (
                PitrCatalogRecord::RetentionSnapshot(snapshot),
                std::mem::size_of::<SegmentMetadata>(),
                std::mem::size_of::<SegmentMetadata>(),
            ),
        ] {
            let boundary_bytes = std::mem::size_of::<ImmutableBoundary>();
            assert_eq!(
                record_owned_bytes(&record),
                common_owned_bytes + boundary_bytes
            );
            let mut validator = ReplayValidator::default();
            validator.apply(&record).unwrap();
            let common_state_bytes =
                decoded_state_bytes(1, &validator, common_owned_bytes).unwrap();
            let encoded = encode_catalog(std::slice::from_ref(&record)).unwrap();
            let tight_limits = PitrCatalogLimits {
                max_decoded_state_bytes: common_state_bytes,
                ..PitrCatalogLimits::default()
            };
            assert!(
                encode_catalog_with_limits(std::slice::from_ref(&record), tight_limits).is_err()
            );
            assert!(replay_catalog_with_limits(&encoded, tight_limits).is_err());
            let exact_limits = PitrCatalogLimits {
                max_decoded_state_bytes: common_state_bytes + boundary_bytes,
                ..PitrCatalogLimits::default()
            };
            assert_eq!(
                encode_catalog_with_limits(std::slice::from_ref(&record), exact_limits).unwrap(),
                encoded
            );
            assert_eq!(
                replay_catalog_with_limits(&encoded, exact_limits)
                    .unwrap()
                    .records,
                vec![record]
            );

            let payload = &encoded[FRAME_HEADER_BYTES..encoded.len() - FRAME_TRAILER_BYTES];
            let error = decode_record(payload, MAX_SNAPSHOT_ITEMS, decoder_base_bytes)
                .unwrap_err()
                .to_string();
            assert!(error.contains("metadata allocation exceeds decoded-state limit"));
            assert!(
                decode_record(
                    payload,
                    MAX_SNAPSHOT_ITEMS,
                    decoder_base_bytes + boundary_bytes
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn large_legacy_catalog_still_fits_default_decoded_state_limit() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let mut predecessor = first.predecessor;
        let records: Vec<_> = (1..=100_000)
            .map(|segment_id| {
                let mut segment = first.clone();
                segment.key.segment_id = SegmentId(segment_id);
                segment.anchor.segment_id = SegmentId(segment_id);
                segment.predecessor = predecessor;
                segment.first_commit_ts = Some(segment_id);
                segment.last_commit_ts = Some(segment_id);
                predecessor = ChainAnchor::Segment(segment.anchor);
                PitrCatalogRecord::CommitSegment { metadata: segment }
            })
            .collect();
        let encoded = encode_catalog(&records).unwrap();
        let replay = replay_catalog(&encoded).unwrap();
        assert_eq!(replay.records, records);
        assert_eq!(replay.sequence, 100_000);
    }

    #[test]
    fn retention_snapshot_supports_replacement_sequence_and_external_chain_start() {
        let predecessor = SegmentAnchor {
            segment_id: SegmentId(1),
            wal_digest: [1; 32],
            seal_digest: [2; 32],
        };
        let metadata = metadata(2, ChainAnchor::Segment(predecessor));
        let snapshot = PitrCatalogRecord::RetentionSnapshot(RetentionSnapshot {
            repository_id: [7; 16],
            replaced_prefix_high_water: 10,
            replaced_prefix_digest: [3; 32],
            chain_starts: vec![RetainedChainStart {
                timeline_id: TimelineId([8; 16]),
                archive_epoch_id: ArchiveEpochId([9; 16]),
                predecessor: ChainAnchor::Segment(predecessor),
            }],
            segments: vec![metadata],
            breaks: Vec::new(),
            retention_cutoff: None,
            oldest_advertised_commit_ts: Some(20),
            backup_catalog_high_water: 2,
            backup_catalog_digest: [4; 32],
        });
        let encoded = encode_catalog(&[snapshot]).unwrap();
        let replay = replay_catalog(&encoded).unwrap();
        assert_eq!(replay.sequence, 11);
    }

    #[test]
    fn replay_rejects_retention_snapshot_after_the_first_frame() {
        let first = metadata(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([9; 16]),
            },
        );
        let first_frame =
            encode_catalog(&[PitrCatalogRecord::CommitSegment { metadata: first }]).unwrap();
        let snapshot = RetentionSnapshot {
            repository_id: [7; 16],
            replaced_prefix_high_water: 1,
            replaced_prefix_digest: [3; 32],
            chain_starts: Vec::new(),
            segments: Vec::new(),
            breaks: Vec::new(),
            retention_cutoff: None,
            oldest_advertised_commit_ts: None,
            backup_catalog_high_water: 0,
            backup_catalog_digest: [0; 32],
        };
        let snapshot_frame =
            encode_catalog(&[PitrCatalogRecord::RetentionSnapshot(snapshot)]).unwrap();
        let mut input = first_frame;
        input.extend_from_slice(&snapshot_frame);
        assert!(replay_catalog(&input).is_err());
    }
}
