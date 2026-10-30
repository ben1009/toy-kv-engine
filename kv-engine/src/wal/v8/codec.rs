//! RFC 026 v8 header, frame, fragment, logical-header and FRONTIER codecs.
//!
//! These codecs are staged independently of WAL creation and recovery. In
//! particular, a valid v8 header does not make the v8 reader or writer
//! available through the WAL registry.

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use crate::{
    pitr::{
        ArchiveEpochId, ChainAnchor, SegmentId, TimelineId, WAL_V5_HEADER_LEN, WAL_V5_MAGIC,
        WAL_V5_VERSION, WalV5Header, WalV5Limits, decode_v5_file_header, encode_v5_file_header,
    },
    wal::format::WAL_FORMAT_VERSION_V8,
};

use super::{WAL_V8_HEADER_LEN, digest};

pub(crate) const WAL_V8_FRAME_LEN: usize = 4096;
pub(crate) const WAL_V8_FRAME_HEADER_LEN: usize = 64;
pub(crate) const WAL_V8_DATA_FRAGMENT_HEADER_LEN: usize = 24;
pub(crate) const WAL_V8_LOGICAL_BATCH_HEADER_LEN: usize = 48;
pub(crate) const WAL_V8_FRONTIER_BODY_LEN: usize = 104;
pub(crate) const WAL_V8_FRAME_BODY_CAPACITY: usize = WAL_V8_FRAME_LEN - WAL_V8_FRAME_HEADER_LEN;
pub(crate) const WAL_V8_DATA_FRAGMENT_PAYLOAD_CAPACITY: usize =
    WAL_V8_FRAME_BODY_CAPACITY - WAL_V8_DATA_FRAGMENT_HEADER_LEN;

const WAL_V8_MIN_LOGICAL_BATCH_LEN: u64 = WAL_V8_LOGICAL_BATCH_HEADER_LEN as u64 + 1;
const WAL_V8_MAX_LOGICAL_BATCH_LEN: u64 = u32::MAX as u64 + WAL_V8_LOGICAL_BATCH_HEADER_LEN as u64;
const WAL_V8_FRAME_MAGIC: [u8; 8] = *b"TKVW8FR1";
const WAL_V8_FRAME_VERSION: u16 = 1;
const WAL_V8_FRAME_KIND_DATA: u16 = 1;
const WAL_V8_FRAME_KIND_FRONTIER: u16 = 2;

/// Count DATA frames needed for a complete logical batch, including its header.
pub(crate) fn data_fragment_count(batch_bytes: u64) -> Option<u32> {
    if !(WAL_V8_MIN_LOGICAL_BATCH_LEN..=WAL_V8_MAX_LOGICAL_BATCH_LEN).contains(&batch_bytes) {
        return None;
    }

    let fragment_capacity = WAL_V8_DATA_FRAGMENT_PAYLOAD_CAPACITY as u64;
    let remainder = u64::from(!batch_bytes.is_multiple_of(fragment_capacity));
    u32::try_from(batch_bytes / fragment_capacity + remainder).ok()
}

/// Immutable v8 segment identity and its physical-header binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8Header {
    pub(crate) timeline_id: TimelineId,
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) segment_id: SegmentId,
    pub(crate) predecessor: ChainAnchor,
    pub(crate) incarnation: [u8; 16],
}

impl WalV8Header {
    /// Encode the complete fixed-size v8 WAL header.
    pub(crate) fn encode(self) -> Result<[u8; WAL_V8_HEADER_LEN]> {
        ensure!(self.incarnation != [0; 16], "v8 WAL incarnation is empty");

        let mut output = encode_v5_file_header(WalV5Header {
            wal_format_version: WAL_V5_VERSION,
            timeline_id: self.timeline_id,
            archive_epoch_id: self.archive_epoch_id,
            segment_id: self.segment_id,
            predecessor: self.predecessor,
        })?;
        output[4..6].copy_from_slice(&WAL_FORMAT_VERSION_V8.to_be_bytes());
        output[128..144].copy_from_slice(&self.incarnation);
        let header_crc = crc32fast::hash(&output[..144]);
        output[144..148].copy_from_slice(&header_crc.to_be_bytes());

        let mut header = [0; WAL_V8_HEADER_LEN];
        header[..WAL_V5_HEADER_LEN].copy_from_slice(&output);
        Ok(header)
    }

    /// Decode and validate a complete v8 WAL header.
    pub(crate) fn decode(input: &[u8]) -> Result<Self> {
        ensure!(input.len() >= WAL_V8_HEADER_LEN, "truncated v8 WAL header");
        let input = &input[..WAL_V8_HEADER_LEN];
        ensure!(input[..4] == WAL_V5_MAGIC, "invalid v8 WAL magic");
        ensure!(
            u16::from_be_bytes(input[4..6].try_into()?) == WAL_FORMAT_VERSION_V8,
            "invalid v8 WAL version"
        );
        ensure!(
            u16::from_be_bytes(input[6..8].try_into()?) == 0,
            "unknown v8 WAL flags"
        );
        ensure!(
            usize::from(u16::from_be_bytes(input[8..10].try_into()?)) == WAL_V8_HEADER_LEN,
            "invalid v8 WAL header length"
        );
        ensure!(
            u16::from_be_bytes(input[10..12].try_into()?) == 0,
            "nonzero v8 WAL reserved field"
        );
        ensure!(
            crc32fast::hash(&input[..144]) == u32::from_be_bytes(input[144..148].try_into()?),
            "v8 WAL header CRC mismatch"
        );
        ensure!(
            input[148..].iter().all(|byte| *byte == 0),
            "nonzero v8 WAL reserved bytes"
        );
        let incarnation: [u8; 16] = input[128..144].try_into()?;
        ensure!(incarnation != [0; 16], "v8 WAL incarnation is empty");

        // Validate shared PITR segment identity fields without allowing the v5
        // decoder to interpret the v8 version or incarnation layout.
        let mut v5_projection = [0_u8; WAL_V5_HEADER_LEN];
        v5_projection[..128].copy_from_slice(&input[..128]);
        v5_projection[4..6].copy_from_slice(&WAL_V5_VERSION.to_be_bytes());
        let projected_header_crc = crc32fast::hash(&v5_projection[..128]);
        v5_projection[128..132].copy_from_slice(&projected_header_crc.to_be_bytes());
        let identity = decode_v5_file_header(&v5_projection)?;

        Ok(Self {
            timeline_id: identity.timeline_id,
            archive_epoch_id: identity.archive_epoch_id,
            segment_id: identity.segment_id,
            predecessor: identity.predecessor,
            incarnation,
        })
    }

    /// Return SHA-256(W), the physical digest bound into every frame header.
    pub(crate) fn physical_digest(self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(self.encode()?).into())
    }

    /// Return RFC 026's distinct logical header seed H = BLAKE3(HEADER || W).
    pub(crate) fn logical_header_seed(self) -> Result<digest::WalV8Digest> {
        Ok(digest::header_digest(&self.encode()?))
    }

    /// Return the v8 generation-zero prefix digest P_0.
    pub(crate) fn initial_prefix_digest(self) -> Result<digest::WalV8Digest> {
        Ok(digest::initial_prefix_digest(&self.logical_header_seed()?))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV8FrameKind {
    Data,
    Frontier,
}

impl WalV8FrameKind {
    fn from_wire(value: u16) -> Result<Self> {
        match value {
            WAL_V8_FRAME_KIND_DATA => Ok(Self::Data),
            WAL_V8_FRAME_KIND_FRONTIER => Ok(Self::Frontier),
            _ => anyhow::bail!("unknown v8 WAL frame kind {value}"),
        }
    }

    const fn to_wire(self) -> u16 {
        match self {
            Self::Data => WAL_V8_FRAME_KIND_DATA,
            Self::Frontier => WAL_V8_FRAME_KIND_FRONTIER,
        }
    }

    fn validate_body_len(self, body_len: usize) -> Result<()> {
        match self {
            Self::Data => ensure!(
                body_len > WAL_V8_DATA_FRAGMENT_HEADER_LEN
                    && body_len <= WAL_V8_FRAME_BODY_CAPACITY,
                "invalid v8 DATA frame body length"
            ),
            Self::Frontier => ensure!(
                body_len == WAL_V8_FRONTIER_BODY_LEN,
                "invalid v8 FRONTIER frame body length"
            ),
        }
        Ok(())
    }

    fn validate_position(
        self,
        body: &[u8],
        initial_prefix_digest: &digest::WalV8Digest,
        frame_offset: u64,
    ) -> Result<()> {
        let is_generation_zero_slot = frame_offset == WAL_V8_HEADER_LEN as u64;
        match self {
            Self::Data => {
                ensure!(
                    !is_generation_zero_slot,
                    "v8 generation-zero slot must contain a FRONTIER frame"
                );
                WalV8DataFragmentHeader::decode_body(body)?;
            }
            Self::Frontier => {
                let frontier = WalV8Frontier::decode_body(body)?;
                if is_generation_zero_slot {
                    frontier.validate_generation_zero(initial_prefix_digest, frame_offset)?;
                } else {
                    ensure!(
                        frontier.generation != 0,
                        "v8 generation-zero FRONTIER is not at offset 4096"
                    );
                    frontier.validate_physical_bounds(
                        frontier.previous_frontier_offset,
                        frame_offset,
                    )?;
                }
            }
        }
        Ok(())
    }
}

/// Frame-local metadata for one fragment of a logical v8 batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8DataFragmentHeader {
    pub(crate) segment_ticket: u64,
    pub(crate) fragment_index: u32,
    pub(crate) fragment_count: u32,
    pub(crate) batch_bytes: u64,
}

impl WalV8DataFragmentHeader {
    pub(crate) fn encode_body(self, payload: &[u8]) -> Result<Vec<u8>> {
        self.validate_payload_len(payload.len())?;
        let body_len = WAL_V8_DATA_FRAGMENT_HEADER_LEN
            .checked_add(payload.len())
            .ok_or_else(|| anyhow::anyhow!("v8 DATA fragment body length overflows"))?;
        ensure!(
            body_len <= WAL_V8_FRAME_BODY_CAPACITY,
            "v8 DATA fragment exceeds frame capacity"
        );

        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&self.segment_ticket.to_be_bytes());
        body.extend_from_slice(&self.fragment_index.to_be_bytes());
        body.extend_from_slice(&self.fragment_count.to_be_bytes());
        body.extend_from_slice(&self.batch_bytes.to_be_bytes());
        body.extend_from_slice(payload);
        Ok(body)
    }

    pub(crate) fn decode_body(input: &[u8]) -> Result<(Self, &[u8])> {
        ensure!(
            input.len() > WAL_V8_DATA_FRAGMENT_HEADER_LEN
                && input.len() <= WAL_V8_FRAME_BODY_CAPACITY,
            "invalid v8 DATA frame body length"
        );
        let header = Self {
            segment_ticket: u64::from_be_bytes(input[0..8].try_into()?),
            fragment_index: u32::from_be_bytes(input[8..12].try_into()?),
            fragment_count: u32::from_be_bytes(input[12..16].try_into()?),
            batch_bytes: u64::from_be_bytes(input[16..24].try_into()?),
        };
        let payload = &input[WAL_V8_DATA_FRAGMENT_HEADER_LEN..];
        header.validate_payload_len(payload.len())?;
        Ok((header, payload))
    }

    fn validate_payload_len(self, payload_len: usize) -> Result<()> {
        ensure!(self.fragment_count > 0, "v8 DATA fragment count is zero");
        ensure!(
            self.fragment_index < self.fragment_count,
            "v8 DATA fragment index is outside its batch"
        );
        ensure!(
            (WAL_V8_MIN_LOGICAL_BATCH_LEN..=WAL_V8_MAX_LOGICAL_BATCH_LEN)
                .contains(&self.batch_bytes),
            "v8 logical batch length is outside wire bounds"
        );
        ensure!(
            data_fragment_count(self.batch_bytes) == Some(self.fragment_count),
            "v8 DATA fragment count does not match logical batch length"
        );

        let fragment_capacity = WAL_V8_DATA_FRAGMENT_PAYLOAD_CAPACITY as u64;
        let batch_offset = u64::from(self.fragment_index)
            .checked_mul(fragment_capacity)
            .ok_or_else(|| anyhow::anyhow!("v8 DATA fragment offset overflows"))?;
        let bytes_remaining = self
            .batch_bytes
            .checked_sub(batch_offset)
            .ok_or_else(|| anyhow::anyhow!("v8 DATA fragment starts past logical batch"))?;
        let expected_payload_len = usize::try_from(bytes_remaining.min(fragment_capacity))?;
        ensure!(
            payload_len == expected_payload_len,
            "v8 DATA fragment payload length does not match its index"
        );
        Ok(())
    }
}

/// The canonical 48-byte logical-batch header used inside v8 DATA fragments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8LogicalBatchHeader {
    pub(crate) segment_ticket: u64,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at_secs: i64,
    pub(crate) recorded_at_nanos: u32,
    pub(crate) entry_count: u32,
}

impl WalV8LogicalBatchHeader {
    pub(crate) fn encode(
        self,
        entry_stream: &[u8],
        limits: WalV5Limits,
    ) -> Result<[u8; WAL_V8_LOGICAL_BATCH_HEADER_LEN]> {
        self.encode_with_data_crc(entry_stream.len(), crc32fast::hash(entry_stream), limits)
    }

    /// Encode the logical header using the CRC accumulated while the canonical
    /// entry stream was emitted. This avoids scanning that stream a second time.
    pub(crate) fn encode_with_data_crc(
        self,
        entry_stream_len: usize,
        entry_stream_crc: u32,
        limits: WalV5Limits,
    ) -> Result<[u8; WAL_V8_LOGICAL_BATCH_HEADER_LEN]> {
        validate_batch_limits(limits)?;
        self.validate(entry_stream_len, limits)?;

        let data_len = u32::try_from(entry_stream_len)?;
        let mut output = [0; WAL_V8_LOGICAL_BATCH_HEADER_LEN];
        output[0..8].copy_from_slice(&self.segment_ticket.to_be_bytes());
        output[8..16].copy_from_slice(&self.commit_ts.to_be_bytes());
        output[16..24].copy_from_slice(&self.recorded_at_secs.to_be_bytes());
        output[24..28].copy_from_slice(&self.recorded_at_nanos.to_be_bytes());
        output[28..32].copy_from_slice(&self.entry_count.to_be_bytes());
        output[32..36].copy_from_slice(&data_len.to_be_bytes());
        output[36..40].copy_from_slice(&entry_stream_crc.to_be_bytes());
        let header_crc = crc32fast::hash(&output[..40]);
        output[40..44].copy_from_slice(&header_crc.to_be_bytes());
        Ok(output)
    }

    pub(crate) fn decode(
        input: &[u8],
        entry_stream: &[u8],
        expected_ticket: u64,
        expected_batch_bytes: u64,
        limits: WalV5Limits,
    ) -> Result<Self> {
        validate_batch_limits(limits)?;
        ensure!(
            input.len() == WAL_V8_LOGICAL_BATCH_HEADER_LEN,
            "invalid v8 logical batch header length"
        );
        let header = Self {
            segment_ticket: u64::from_be_bytes(input[0..8].try_into()?),
            commit_ts: u64::from_be_bytes(input[8..16].try_into()?),
            recorded_at_secs: i64::from_be_bytes(input[16..24].try_into()?),
            recorded_at_nanos: u32::from_be_bytes(input[24..28].try_into()?),
            entry_count: u32::from_be_bytes(input[28..32].try_into()?),
        };
        let data_len = usize::try_from(u32::from_be_bytes(input[32..36].try_into()?))?;
        header.validate(data_len, limits)?;
        ensure!(
            entry_stream.len() == data_len,
            "v8 logical batch data length does not match its header"
        );
        let actual_batch_bytes = u64::try_from(WAL_V8_LOGICAL_BATCH_HEADER_LEN)?
            .checked_add(u64::try_from(data_len)?)
            .ok_or_else(|| anyhow::anyhow!("v8 logical batch length overflows"))?;
        ensure!(
            actual_batch_bytes == expected_batch_bytes,
            "v8 logical batch length does not match DATA fragments"
        );
        ensure!(
            header.segment_ticket == expected_ticket,
            "v8 logical batch ticket does not match DATA fragments"
        );
        ensure!(
            u32::from_be_bytes(input[36..40].try_into()?) == crc32fast::hash(entry_stream),
            "v8 logical batch data CRC mismatch"
        );
        ensure!(
            u32::from_be_bytes(input[40..44].try_into()?) == crc32fast::hash(&input[..40]),
            "v8 logical batch header CRC mismatch"
        );
        ensure!(
            input[44..48].iter().all(|byte| *byte == 0),
            "nonzero v8 logical batch reserved field"
        );
        Ok(header)
    }

    fn validate(self, entry_stream_len: usize, limits: WalV5Limits) -> Result<()> {
        ensure!(
            self.commit_ts > 0,
            "v8 logical batch commit timestamp is zero"
        );
        ensure!(self.entry_count > 0, "v8 logical batch entry count is zero");
        ensure!(
            usize::try_from(self.entry_count)? <= limits.max_entry_count,
            "v8 logical batch entry count exceeds configured limit"
        );
        ensure!(
            entry_stream_len > 0 && entry_stream_len <= limits.max_batch_data_bytes,
            "v8 logical batch data length is outside configured limits"
        );
        ensure!(
            u32::try_from(entry_stream_len).is_ok(),
            "v8 logical batch data length exceeds wire format"
        );
        ensure!(
            self.recorded_at_nanos < 1_000_000_000,
            "v8 recorded_at nanos out of range"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedWalV8Frame {
    pub(crate) kind: WalV8FrameKind,
    pub(crate) physical_header_digest: [u8; 32],
    pub(crate) frame_offset: u64,
    pub(crate) body: Vec<u8>,
}

pub(crate) fn is_frontier_frame_header(input: &[u8]) -> bool {
    input.len() >= 12
        && input[..8] == WAL_V8_FRAME_MAGIC
        && u16::from_be_bytes([input[10], input[11]]) == WAL_V8_FRAME_KIND_FRONTIER
}

/// Encode a frame with the v8 common 64-byte envelope and zero padding.
fn encode_frame(
    kind: WalV8FrameKind,
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
    frame_offset: u64,
    body: &[u8],
) -> Result<[u8; WAL_V8_FRAME_LEN]> {
    let mut output = [0_u8; WAL_V8_FRAME_LEN];
    encode_frame_into(
        kind,
        physical_header_digest,
        initial_prefix_digest,
        frame_offset,
        body.len(),
        &mut output,
        |body_output| {
            body_output.copy_from_slice(body);
            Ok(())
        },
    )?;
    Ok(output)
}

/// Encode a complete frame directly into a caller-owned aligned frame buffer.
/// The body closure fills only the advertised body bytes; remaining frame bytes
/// are zero padding and are included in the frame CRC.
fn encode_frame_into<F>(
    kind: WalV8FrameKind,
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
    frame_offset: u64,
    body_len: usize,
    output: &mut [u8],
    fill_body: F,
) -> Result<()>
where
    F: FnOnce(&mut [u8]) -> Result<()>,
{
    kind.validate_body_len(body_len)?;
    validate_frame_offset(frame_offset)?;
    ensure!(
        output.len() == WAL_V8_FRAME_LEN,
        "v8 frame output buffer must be exactly one frame"
    );
    let body_len_u32 = u32::try_from(body_len)?;

    output.fill(0);
    output[..8].copy_from_slice(&WAL_V8_FRAME_MAGIC);
    output[8..10].copy_from_slice(&WAL_V8_FRAME_VERSION.to_be_bytes());
    output[10..12].copy_from_slice(&kind.to_wire().to_be_bytes());
    output[12..14].copy_from_slice(&(WAL_V8_FRAME_HEADER_LEN as u16).to_be_bytes());
    output[16..48].copy_from_slice(&physical_header_digest);
    output[48..56].copy_from_slice(&frame_offset.to_be_bytes());
    output[56..60].copy_from_slice(&body_len_u32.to_be_bytes());

    let body_end = WAL_V8_FRAME_HEADER_LEN
        .checked_add(body_len)
        .context("v8 frame body end overflows")?;
    let body = &mut output[WAL_V8_FRAME_HEADER_LEN..body_end];
    fill_body(body)?;
    kind.validate_position(body, &initial_prefix_digest, frame_offset)?;

    let crc = frame_crc(output);
    output[60..64].copy_from_slice(&crc.to_be_bytes());
    Ok(())
}

pub(crate) fn encode_data_frame(
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
    frame_offset: u64,
    body: &[u8],
) -> Result<[u8; WAL_V8_FRAME_LEN]> {
    encode_frame(
        WalV8FrameKind::Data,
        physical_header_digest,
        initial_prefix_digest,
        frame_offset,
        body,
    )
}

/// Encode a DATA fragment directly into its final frame, avoiding temporary
/// fragment-body and full-frame allocations.
pub(crate) fn encode_data_fragment_frame_into(
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
    frame_offset: u64,
    fragment: WalV8DataFragmentHeader,
    payload: &[u8],
    output: &mut [u8],
) -> Result<()> {
    fragment.validate_payload_len(payload.len())?;
    let body_len = WAL_V8_DATA_FRAGMENT_HEADER_LEN
        .checked_add(payload.len())
        .context("v8 DATA fragment body length overflows")?;
    encode_frame_into(
        WalV8FrameKind::Data,
        physical_header_digest,
        initial_prefix_digest,
        frame_offset,
        body_len,
        output,
        |body| {
            body[0..8].copy_from_slice(&fragment.segment_ticket.to_be_bytes());
            body[8..12].copy_from_slice(&fragment.fragment_index.to_be_bytes());
            body[12..16].copy_from_slice(&fragment.fragment_count.to_be_bytes());
            body[16..24].copy_from_slice(&fragment.batch_bytes.to_be_bytes());
            body[WAL_V8_DATA_FRAGMENT_HEADER_LEN..].copy_from_slice(payload);
            Ok(())
        },
    )
}

pub(crate) fn encode_generation_zero_frontier(
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
) -> Result<[u8; WAL_V8_FRAME_LEN]> {
    let frontier = WalV8Frontier {
        generation: 0,
        ticket_end: 0,
        durable_end: WAL_V8_HEADER_LEN as u64,
        last_commit_ts: 0,
        prefix_digest: initial_prefix_digest,
        previous_frontier_offset: 0,
        previous_frontier_digest: [0; 32],
    };
    encode_frame(
        WalV8FrameKind::Frontier,
        physical_header_digest,
        initial_prefix_digest,
        WAL_V8_HEADER_LEN as u64,
        &frontier.encode_body(),
    )
}

/// Encode a nonzero FRONTIER after validating its logical and physical link to
/// the previously accepted frontier.
pub(crate) fn encode_frontier_successor(
    frontier: WalV8Frontier,
    previous: WalV8Frontier,
    previous_frame_offset: u64,
    previous_frame_digest: &[u8; 32],
    frame_offset: u64,
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
) -> Result<[u8; WAL_V8_FRAME_LEN]> {
    frontier.validate_successor(
        previous,
        previous_frame_offset,
        previous_frame_digest,
        frame_offset,
        &physical_header_digest,
        &initial_prefix_digest,
    )?;
    encode_frame(
        WalV8FrameKind::Frontier,
        physical_header_digest,
        initial_prefix_digest,
        frame_offset,
        &frontier.encode_body(),
    )
}

/// Decode and validate a frame envelope. This does not prove DATA fragment
/// sequence completeness or a nonzero FRONTIER's predecessor chain.
pub(crate) fn decode_frame_structural(
    input: &[u8],
    actual_offset: u64,
    expected_physical_header_digest: &[u8; 32],
    initial_prefix_digest: &digest::WalV8Digest,
) -> Result<DecodedWalV8Frame> {
    ensure!(
        input.len() == WAL_V8_FRAME_LEN,
        "invalid v8 WAL frame length"
    );
    validate_frame_offset(actual_offset)?;
    ensure!(
        input[..8] == WAL_V8_FRAME_MAGIC,
        "invalid v8 WAL frame magic"
    );
    ensure!(
        u16::from_be_bytes(input[8..10].try_into()?) == WAL_V8_FRAME_VERSION,
        "unsupported v8 WAL frame version"
    );
    let kind = WalV8FrameKind::from_wire(u16::from_be_bytes(input[10..12].try_into()?))?;
    ensure!(
        usize::from(u16::from_be_bytes(input[12..14].try_into()?)) == WAL_V8_FRAME_HEADER_LEN,
        "invalid v8 WAL frame header length"
    );
    ensure!(
        u16::from_be_bytes(input[14..16].try_into()?) == 0,
        "unknown v8 WAL frame flags"
    );
    let physical_header_digest: [u8; 32] = input[16..48].try_into()?;
    ensure!(
        &physical_header_digest == expected_physical_header_digest,
        "v8 WAL frame header digest mismatch"
    );
    ensure!(
        u64::from_be_bytes(input[48..56].try_into()?) == actual_offset,
        "v8 WAL frame offset mismatch"
    );
    let body_len = usize::try_from(u32::from_be_bytes(input[56..60].try_into()?))?;
    kind.validate_body_len(body_len)?;
    ensure!(
        frame_crc(input) == u32::from_be_bytes(input[60..64].try_into()?),
        "v8 WAL frame CRC mismatch"
    );
    let body_end = WAL_V8_FRAME_HEADER_LEN
        .checked_add(body_len)
        .context("v8 WAL frame body range overflows")?;
    ensure!(
        input[body_end..].iter().all(|byte| *byte == 0),
        "nonzero v8 WAL frame padding"
    );
    let body = &input[WAL_V8_FRAME_HEADER_LEN..body_end];
    kind.validate_position(body, initial_prefix_digest, actual_offset)?;

    Ok(DecodedWalV8Frame {
        kind,
        physical_header_digest,
        frame_offset: actual_offset,
        body: body.to_vec(),
    })
}

pub(crate) fn frame_record_digest(frame: &[u8]) -> Result<[u8; 32]> {
    ensure!(
        frame.len() == WAL_V8_FRAME_LEN,
        "invalid v8 WAL frame length"
    );
    Ok(Sha256::digest(frame).into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8Frontier {
    pub(crate) generation: u64,
    pub(crate) ticket_end: u64,
    pub(crate) durable_end: u64,
    pub(crate) last_commit_ts: u64,
    pub(crate) prefix_digest: digest::WalV8Digest,
    pub(crate) previous_frontier_offset: u64,
    pub(crate) previous_frontier_digest: [u8; 32],
}

impl WalV8Frontier {
    pub(crate) fn encode_body(self) -> [u8; WAL_V8_FRONTIER_BODY_LEN] {
        let mut output = [0; WAL_V8_FRONTIER_BODY_LEN];
        output[0..8].copy_from_slice(&self.generation.to_be_bytes());
        output[8..16].copy_from_slice(&self.ticket_end.to_be_bytes());
        output[16..24].copy_from_slice(&self.durable_end.to_be_bytes());
        output[24..32].copy_from_slice(&self.last_commit_ts.to_be_bytes());
        output[32..64].copy_from_slice(&self.prefix_digest);
        output[64..72].copy_from_slice(&self.previous_frontier_offset.to_be_bytes());
        output[72..104].copy_from_slice(&self.previous_frontier_digest);
        output
    }

    pub(crate) fn decode_body(input: &[u8]) -> Result<Self> {
        ensure!(
            input.len() == WAL_V8_FRONTIER_BODY_LEN,
            "invalid v8 FRONTIER body length"
        );
        Ok(Self {
            generation: u64::from_be_bytes(input[0..8].try_into()?),
            ticket_end: u64::from_be_bytes(input[8..16].try_into()?),
            durable_end: u64::from_be_bytes(input[16..24].try_into()?),
            last_commit_ts: u64::from_be_bytes(input[24..32].try_into()?),
            prefix_digest: input[32..64].try_into()?,
            previous_frontier_offset: u64::from_be_bytes(input[64..72].try_into()?),
            previous_frontier_digest: input[72..104].try_into()?,
        })
    }

    pub(crate) fn validate_generation_zero(
        self,
        expected_initial_prefix_digest: &digest::WalV8Digest,
        frame_offset: u64,
    ) -> Result<()> {
        ensure!(
            frame_offset == WAL_V8_HEADER_LEN as u64,
            "v8 generation-zero FRONTIER is not at offset 4096"
        );
        ensure!(
            self.generation == 0
                && self.ticket_end == 0
                && self.durable_end == WAL_V8_HEADER_LEN as u64
                && self.last_commit_ts == 0
                && self.prefix_digest == *expected_initial_prefix_digest
                && self.previous_frontier_offset == 0
                && self.previous_frontier_digest == [0; 32],
            "invalid v8 generation-zero FRONTIER"
        );
        Ok(())
    }

    pub(crate) fn validate_successor(
        self,
        previous: Self,
        previous_frame_offset: u64,
        previous_frame_digest: &[u8; 32],
        frame_offset: u64,
        physical_header_digest: &[u8; 32],
        initial_prefix_digest: &digest::WalV8Digest,
    ) -> Result<()> {
        if previous.generation == 0 {
            previous.validate_generation_zero(initial_prefix_digest, previous_frame_offset)?;
            let generation_zero_frame =
                encode_generation_zero_frontier(*physical_header_digest, *initial_prefix_digest)?;
            ensure!(
                frame_record_digest(&generation_zero_frame)? == *previous_frame_digest,
                "v8 generation-zero FRONTIER digest mismatch"
            );
        }

        self.validate_physical_bounds(previous_frame_offset, frame_offset)?;
        let expected_generation = previous
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("v8 FRONTIER generation overflows"))?;
        ensure!(
            self.generation == expected_generation,
            "v8 FRONTIER generation is not consecutive"
        );
        ensure!(
            self.ticket_end > previous.ticket_end
                && self.durable_end > previous.durable_end
                && self.last_commit_ts > previous.last_commit_ts,
            "v8 FRONTIER logical boundary did not advance"
        );
        ensure!(
            self.previous_frontier_offset == previous_frame_offset
                && self.previous_frontier_digest == *previous_frame_digest,
            "v8 FRONTIER predecessor binding mismatch"
        );
        Ok(())
    }

    fn validate_physical_bounds(self, previous_frame_offset: u64, frame_offset: u64) -> Result<()> {
        validate_frame_offset(previous_frame_offset)?;
        validate_frame_offset(frame_offset)?;
        let previous_frame_end = previous_frame_offset
            .checked_add(WAL_V8_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("v8 previous FRONTIER offset overflows"))?;
        ensure!(
            previous_frame_offset < frame_offset
                && previous_frame_end <= self.durable_end
                && self.durable_end <= frame_offset
                && self.durable_end.is_multiple_of(WAL_V8_FRAME_LEN as u64),
            "invalid v8 FRONTIER physical bounds"
        );
        Ok(())
    }
}

pub(crate) fn decode_frontier_successor(
    input: &[u8],
    actual_offset: u64,
    expected_physical_header_digest: &[u8; 32],
    initial_prefix_digest: &digest::WalV8Digest,
    previous: WalV8Frontier,
    previous_frame_offset: u64,
    previous_frame_digest: &[u8; 32],
) -> Result<(WalV8Frontier, [u8; 32])> {
    let frame = decode_frame_structural(
        input,
        actual_offset,
        expected_physical_header_digest,
        initial_prefix_digest,
    )?;
    ensure!(
        frame.kind == WalV8FrameKind::Frontier,
        "v8 successor decoder received a DATA frame"
    );
    let frontier = WalV8Frontier::decode_body(&frame.body)?;
    frontier.validate_successor(
        previous,
        previous_frame_offset,
        previous_frame_digest,
        actual_offset,
        expected_physical_header_digest,
        initial_prefix_digest,
    )?;
    Ok((frontier, frame_record_digest(input)?))
}

fn validate_frame_offset(frame_offset: u64) -> Result<()> {
    ensure!(
        frame_offset >= WAL_V8_HEADER_LEN as u64
            && frame_offset.is_multiple_of(WAL_V8_FRAME_LEN as u64),
        "unaligned v8 WAL frame offset"
    );
    frame_offset
        .checked_add(WAL_V8_FRAME_LEN as u64)
        .ok_or_else(|| anyhow::anyhow!("v8 WAL frame end overflows"))?;
    Ok(())
}

fn validate_batch_limits(limits: WalV5Limits) -> Result<()> {
    ensure!(
        limits.max_entry_count > 0,
        "v8 entry count limit must be nonzero"
    );
    ensure!(
        limits.max_batch_data_bytes > 0,
        "v8 batch byte limit must be nonzero"
    );
    ensure!(
        limits.max_batch_data_bytes <= u32::MAX as usize,
        "v8 batch byte limit exceeds wire format"
    );
    Ok(())
}

fn frame_crc(frame: &[u8]) -> u32 {
    let mut crc = crc32fast::Hasher::new();
    crc.update(&frame[..60]);
    crc.update(&frame[64..]);
    crc.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pitr::LIVE_WAL_V5_LIMITS;

    const PHYSICAL_HEADER_DIGEST: &str =
        "9439ee534fbcc668119f7fa9b5a7ab1b26140318b059bb57588a58eca0d37abd";
    const LOGICAL_HEADER_SEED: &str =
        "1f2f23b2b7b861b75b908516c230301890e432632652ee1671c9dc697d53708d";
    const VALID_P0: &str = "56bf0f5971fff54b5f4d11e2cb2b9dd4d51c6ba82ee691966008ab267c7089a6";
    const VALID_P1: &str = "9effca1cceea2658a7a1fb4b4a1f2e03ce5a2f97ec1a609b57a2312e4408a79d";
    const F0_BODY_HEX: &str = concat!(
        "0000000000000000",
        "0000000000000000",
        "0000000000001000",
        "0000000000000000",
        "56bf0f5971fff54b5f4d11e2cb2b9dd4d51c6ba82ee691966008ab267c7089a6",
        "0000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    const DATA_LOGICAL_HEADER_HEX: &str = concat!(
        "0102030405060708",
        "0000000000000001",
        "fffffffffffffffe",
        "1dcd6500",
        "00000001",
        "0000000b",
        "a05d100f",
        "881dce8b",
        "00000000"
    );
    const DATA_BODY_HEX: &str = concat!(
        "0102030405060708",
        "00000000",
        "00000001",
        "000000000000003b",
        "01020304050607080000000000000001fffffffffffffffe1dcd650000000001",
        "0000000ba05d100f881dce8b00000000",
        "020000000005000000016b"
    );
    const F1_BODY_HEX: &str = concat!(
        "0000000000000001",
        "0000000000000001",
        "0000000000003000",
        "0000000000000001",
        "9effca1cceea2658a7a1fb4b4a1f2e03",
        "ce5a2f97ec1a609b57a2312e4408a79d",
        "0000000000001000",
        "eb237a76a56c9f70cca15a7d40adfc9739034e95a178d779820ff7dbf284b46c"
    );

    fn golden_header() -> WalV8Header {
        let archive_epoch_id = ArchiveEpochId([
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
            0x2e, 0x2f,
        ]);
        WalV8Header {
            timeline_id: TimelineId([
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
                0x1e, 0x1f,
            ]),
            archive_epoch_id,
            segment_id: SegmentId(0x0102_0304_0506_0708),
            predecessor: ChainAnchor::Genesis { archive_epoch_id },
            incarnation: [
                0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d,
                0x3e, 0x3f,
            ],
        }
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty());
        pairs
            .iter()
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                    .expect("valid hex")
            })
            .collect()
    }

    fn decode_array<const N: usize>(value: &str) -> [u8; N] {
        decode_hex(value).try_into().expect("golden byte length")
    }

    fn golden_frame(
        kind: u16,
        physical_header_digest: &[u8; 32],
        frame_offset: u64,
        body: &[u8],
        crc: [u8; 4],
    ) -> [u8; WAL_V8_FRAME_LEN] {
        let mut frame = [0; WAL_V8_FRAME_LEN];
        frame[..8].copy_from_slice(b"TKVW8FR1");
        frame[8..10].copy_from_slice(&[0, 1]);
        frame[10..12].copy_from_slice(&kind.to_be_bytes());
        frame[12..14].copy_from_slice(&[0, 64]);
        frame[16..48].copy_from_slice(physical_header_digest);
        frame[48..56].copy_from_slice(&frame_offset.to_be_bytes());
        frame[56..60].copy_from_slice(&u32::try_from(body.len()).unwrap().to_be_bytes());
        frame[60..64].copy_from_slice(&crc);
        frame[64..64 + body.len()].copy_from_slice(body);
        frame
    }

    #[test]
    fn v8_header_and_generation_zero_frontier_match_golden_bytes() -> Result<()> {
        let header = golden_header();
        let encoded = header.encode()?;
        let mut expected = [0; WAL_V8_HEADER_LEN];
        expected[..4].copy_from_slice(b"WAL2");
        expected[4..6].copy_from_slice(&[0, 8]);
        expected[8..10].copy_from_slice(&[0x10, 0]);
        expected[12..28].copy_from_slice(&[
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f,
        ]);
        expected[28..44].copy_from_slice(&[
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
            0x2e, 0x2f,
        ]);
        expected[44..52].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        expected[128..144].copy_from_slice(&[
            0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d,
            0x3e, 0x3f,
        ]);
        let header_crc = crc32fast::hash(&expected[..144]);
        assert_eq!(header_crc, 0xb97f_e4f2);
        expected[144..148].copy_from_slice(&header_crc.to_be_bytes());
        assert_eq!(encoded, expected);
        assert_eq!(WalV8Header::decode(&expected)?, header);

        let physical_digest = header.physical_digest()?;
        let logical_seed = header.logical_header_seed()?;
        let p0 = header.initial_prefix_digest()?;
        assert_ne!(physical_digest.as_slice(), p0.as_slice());
        assert_eq!(physical_digest, decode_array::<32>(PHYSICAL_HEADER_DIGEST));
        assert_eq!(logical_seed, decode_array::<32>(LOGICAL_HEADER_SEED));
        assert_eq!(p0, decode_array::<32>(VALID_P0));

        let frontier = WalV8Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: WAL_V8_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: p0,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let body = frontier.encode_body();
        assert_eq!(body.as_slice(), decode_hex(F0_BODY_HEX));
        let frame = encode_generation_zero_frontier(physical_digest, p0)?;
        let expected_frame =
            golden_frame(2, &physical_digest, 4096, &body, [0xe6, 0x2b, 0x21, 0xe3]);
        assert_eq!(frame, expected_frame);
        assert_eq!(
            frame_record_digest(&frame)?,
            decode_array::<32>("eb237a76a56c9f70cca15a7d40adfc9739034e95a178d779820ff7dbf284b46c")
        );
        assert_eq!(WalV8Frontier::decode_body(&body)?, frontier);
        assert_eq!(
            decode_frame_structural(&frame, 4096, &physical_digest, &p0)?.body,
            body
        );
        Ok(())
    }

    #[test]
    fn v8_data_fragment_round_trips_fragment_boundaries_and_logical_header() -> Result<()> {
        let ticket = 0x0102_0304_0506_0708;
        let header = WalV8LogicalBatchHeader {
            segment_ticket: ticket,
            commit_ts: 1,
            recorded_at_secs: -2,
            recorded_at_nanos: 500_000_000,
            entry_count: 1,
        };
        let entry_stream = decode_hex("020000000005000000016b");
        let logical_header = header.encode(&entry_stream, LIVE_WAL_V5_LIMITS)?;
        assert_eq!(
            logical_header.as_slice(),
            decode_hex(DATA_LOGICAL_HEADER_HEX)
        );
        let mut logical_batch = logical_header.to_vec();
        logical_batch.extend_from_slice(&entry_stream);
        let fragment_header = WalV8DataFragmentHeader {
            segment_ticket: ticket,
            fragment_index: 0,
            fragment_count: 1,
            batch_bytes: u64::try_from(logical_batch.len())?,
        };
        let body = fragment_header.encode_body(&logical_batch)?;
        assert_eq!(body, decode_hex(DATA_BODY_HEX));
        let (decoded_fragment, payload) = WalV8DataFragmentHeader::decode_body(&body)?;
        assert_eq!(decoded_fragment, fragment_header);
        assert_eq!(payload, logical_batch);
        assert_eq!(
            WalV8LogicalBatchHeader::decode(
                &payload[..WAL_V8_LOGICAL_BATCH_HEADER_LEN],
                &payload[WAL_V8_LOGICAL_BATCH_HEADER_LEN..],
                ticket,
                u64::try_from(payload.len())?,
                LIVE_WAL_V5_LIMITS,
            )?,
            header
        );

        let wal_header = golden_header();
        let physical_digest = wal_header.physical_digest()?;
        let p0 = wal_header.initial_prefix_digest()?;
        let frame = encode_data_frame(physical_digest, p0, 8192, &body)?;
        let expected_frame =
            golden_frame(1, &physical_digest, 8192, &body, [0x33, 0x0e, 0xb3, 0xc0]);
        assert_eq!(frame, expected_frame);
        assert_eq!(
            frame_record_digest(&frame)?,
            decode_array::<32>("70ff4aeceea960e5cfd73f598da493dc524be66ddb90a6c60dee83fd4994621c")
        );
        let decoded = decode_frame_structural(&frame, 8192, &physical_digest, &p0)?;
        assert_eq!(decoded.kind, WalV8FrameKind::Data);
        assert_eq!(decoded.body, body);

        for (batch_len, expected_fragments, last_payload_len) in [
            (4007, 1, 4007),
            (4008, 1, 4008),
            (4009, 2, 1),
            (8015, 2, 4007),
            (8016, 2, 4008),
            (8017, 3, 1),
        ] {
            assert_eq!(data_fragment_count(batch_len), Some(expected_fragments));
            let last_index = expected_fragments - 1;
            let body = WalV8DataFragmentHeader {
                segment_ticket: 0,
                fragment_index: last_index,
                fragment_count: expected_fragments,
                batch_bytes: batch_len,
            }
            .encode_body(&vec![0x5a; last_payload_len])?;
            let (decoded, payload) = WalV8DataFragmentHeader::decode_body(&body)?;
            assert_eq!(decoded.fragment_index, last_index);
            assert_eq!(payload.len(), last_payload_len);
        }
        Ok(())
    }

    #[test]
    fn v8_frontier_successor_binds_physical_f0_and_logical_prefix() -> Result<()> {
        let header = golden_header();
        let physical_digest = header.physical_digest()?;
        let p0 = header.initial_prefix_digest()?;
        let logical_batch_header = decode_array::<48>(concat!(
            "00000000000000000000000000000001fffffffffffffffd1dcd650000000001",
            "00000010c445932542a3b0b900000000"
        ));
        let entry_stream = decode_hex("01000000000a000000016b0000000176");
        let logical = WalV8LogicalBatchHeader::decode(
            &logical_batch_header,
            &entry_stream,
            0,
            64,
            LIVE_WAL_V5_LIMITS,
        )?;
        assert_eq!(logical.commit_ts, 1);
        let mut entry_hasher = digest::WalV8EntryHasher::new(u64::try_from(entry_stream.len())?);
        entry_hasher.update(&entry_stream)?;
        let entries_digest = entry_hasher.finish()?;
        assert_eq!(
            entries_digest,
            decode_array::<32>("15c56b2a4fef9ab6690b5f8c68e10b8750ad63a677951adcc5a3aeefa3f45fb4")
        );
        let batch = digest::batch_digest(&logical_batch_header, &entries_digest);
        assert_eq!(
            batch,
            decode_array::<32>("dca94ad72c4cc0bd3c4e139afc1e4fed6cb3b08b316a29f1e0f5e07b0f123f24")
        );
        let p1 = digest::advance_prefix(&p0, 0, 64, &batch);
        assert_eq!(p1, decode_array::<32>(VALID_P1));

        let f0_bytes = encode_generation_zero_frontier(physical_digest, p0)?;
        let f0 = WalV8Frontier::decode_body(
            &decode_frame_structural(&f0_bytes, 4096, &physical_digest, &p0)?.body,
        )?;
        let f0_digest = frame_record_digest(&f0_bytes)?;
        let f1 = WalV8Frontier {
            generation: 1,
            ticket_end: 1,
            durable_end: 12_288,
            last_commit_ts: 1,
            prefix_digest: p1,
            previous_frontier_offset: 4096,
            previous_frontier_digest: f0_digest,
        };
        let f1_bytes =
            encode_frontier_successor(f1, f0, 4096, &f0_digest, 12_288, physical_digest, p0)?;
        assert_eq!(f1.encode_body().as_slice(), decode_hex(F1_BODY_HEX));
        let expected_f1_frame = golden_frame(
            2,
            &physical_digest,
            12_288,
            &decode_hex(F1_BODY_HEX),
            [0x7f, 0x82, 0x79, 0xe0],
        );
        assert_eq!(f1_bytes, expected_f1_frame);
        assert_eq!(
            frame_record_digest(&f1_bytes)?,
            decode_array::<32>("ee951e609983950a76a52f8be35a39acdccc2c8249dacbe0f8fb424f13e7531d")
        );
        let (decoded_f1, decoded_digest) = decode_frontier_successor(
            &f1_bytes,
            12_288,
            &physical_digest,
            &p0,
            f0,
            4096,
            &f0_digest,
        )?;
        assert_eq!(decoded_f1, f1);
        assert_eq!(decoded_digest, frame_record_digest(&f1_bytes)?);

        let wrong_f0_digest = [0x11; 32];
        assert!(
            decode_frontier_successor(
                &f1_bytes,
                12_288,
                &physical_digest,
                &p0,
                f0,
                4096,
                &wrong_f0_digest,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn v8_header_rejects_mutations_to_reserved_bytes_and_crc() -> Result<()> {
        let mut header = golden_header().encode()?;
        header[200] = 1;
        assert!(WalV8Header::decode(&header).is_err());

        let mut header = golden_header().encode()?;
        header[4] = 7;
        assert!(WalV8Header::decode(&header).is_err());

        let mut header = golden_header().encode()?;
        header[144] ^= 1;
        assert!(WalV8Header::decode(&header).is_err());
        Ok(())
    }

    #[test]
    fn v8_frontier_predecessor_and_frame_validation_reject_corruption() -> Result<()> {
        let header = golden_header();
        let physical_digest = header.physical_digest()?;
        let p0 = header.initial_prefix_digest()?;
        let f0_bytes = encode_generation_zero_frontier(physical_digest, p0)?;
        let f0 = WalV8Frontier::decode_body(
            &decode_frame_structural(&f0_bytes, 4096, &physical_digest, &p0)?.body,
        )?;
        let f0_digest = frame_record_digest(&f0_bytes)?;
        let mut f1 = WalV8Frontier {
            generation: 1,
            ticket_end: 1,
            durable_end: 12_288,
            last_commit_ts: 1,
            prefix_digest: [0x22; 32],
            previous_frontier_offset: 4096,
            previous_frontier_digest: f0_digest,
        };
        f1.previous_frontier_offset = 8192;
        assert!(
            f1.validate_successor(f0, 4096, &f0_digest, 12_288, &physical_digest, &p0)
                .is_err()
        );

        let mut f0_corrupt = f0_bytes;
        f0_corrupt[100] ^= 1;
        assert!(decode_frame_structural(&f0_corrupt, 4096, &physical_digest, &p0).is_err());
        assert!(decode_frame_structural(&f0_bytes, 8192, &physical_digest, &p0).is_err());
        assert!(decode_frame_structural(&f0_bytes, 4096, &[0; 32], &p0).is_err());
        Ok(())
    }
}
