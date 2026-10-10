use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

use crate::{
    pitr::{
        ArchiveEpochId, ChainAnchor, SegmentId, TimelineId, WAL_V5_HEADER_LEN, WAL_V5_MAGIC,
        WAL_V5_VERSION, WalV5Header, WalV5Limits, decode_v5_file_header, encode_v5_file_header,
    },
    wal::format::WAL_FORMAT_VERSION_V7,
};

pub(crate) const WAL_V7_HEADER_LEN: usize = 4096;
pub(crate) const WAL_V7_FRAME_LEN: usize = 4096;
pub(crate) const WAL_V7_FRAME_HEADER_LEN: usize = 64;
pub(crate) const WAL_V7_DATA_FRAGMENT_HEADER_LEN: usize = 24;
pub(crate) const WAL_V7_LOGICAL_BATCH_HEADER_LEN: usize = 48;
pub(crate) const WAL_V7_FRONTIER_BODY_LEN: usize = 104;
pub(crate) const WAL_V7_FRAME_BODY_CAPACITY: usize = WAL_V7_FRAME_LEN - WAL_V7_FRAME_HEADER_LEN;
pub(crate) const WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY: usize =
    WAL_V7_FRAME_BODY_CAPACITY - WAL_V7_DATA_FRAGMENT_HEADER_LEN;

const WAL_V7_MIN_LOGICAL_BATCH_LEN: u64 = WAL_V7_LOGICAL_BATCH_HEADER_LEN as u64 + 1;
const WAL_V7_MAX_LOGICAL_BATCH_LEN: u64 = u32::MAX as u64 + WAL_V7_LOGICAL_BATCH_HEADER_LEN as u64;

/// Count DATA frames for the complete logical batch, including its header.
/// Returns `None` when the length is outside the v7 wire envelope.
pub(crate) fn data_fragment_count(batch_bytes: u64) -> Option<u32> {
    if !(WAL_V7_MIN_LOGICAL_BATCH_LEN..=WAL_V7_MAX_LOGICAL_BATCH_LEN).contains(&batch_bytes) {
        return None;
    }

    let fragment_capacity = WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY as u64;
    let remainder = if batch_bytes.is_multiple_of(fragment_capacity) {
        0
    } else {
        1
    };
    let fragment_count = batch_bytes / fragment_capacity + remainder;

    u32::try_from(fragment_count).ok()
}

const WAL_V7_FRAME_MAGIC: [u8; 8] = *b"TKVW7FR1";
const WAL_V7_FRAME_VERSION: u16 = 1;
const WAL_V7_FRAME_KIND_DATA: u16 = 1;
const WAL_V7_FRAME_KIND_FRONTIER: u16 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV7Header {
    pub(crate) timeline_id: TimelineId,
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) segment_id: SegmentId,
    pub(crate) predecessor: ChainAnchor,
    pub(crate) incarnation: [u8; 16],
}

impl WalV7Header {
    pub(crate) fn encode(self) -> Result<[u8; WAL_V7_HEADER_LEN]> {
        ensure!(self.incarnation != [0; 16], "v7 WAL incarnation is empty");

        let mut output = encode_v5_file_header(WalV5Header {
            wal_format_version: WAL_V5_VERSION,
            timeline_id: self.timeline_id,
            archive_epoch_id: self.archive_epoch_id,
            segment_id: self.segment_id,
            predecessor: self.predecessor,
        })?;
        output[4..6].copy_from_slice(&WAL_FORMAT_VERSION_V7.to_be_bytes());
        output[128..144].copy_from_slice(&self.incarnation);
        let header_crc = crc32fast::hash(&output[..144]);
        output[144..148].copy_from_slice(&header_crc.to_be_bytes());

        Ok(output)
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self> {
        ensure!(input.len() >= WAL_V7_HEADER_LEN, "truncated v7 WAL header");
        let input = &input[..WAL_V7_HEADER_LEN];
        ensure!(input[..4] == WAL_V5_MAGIC, "invalid v7 WAL magic");
        ensure!(
            u16::from_be_bytes(input[4..6].try_into()?) == WAL_FORMAT_VERSION_V7,
            "invalid v7 WAL version"
        );
        ensure!(
            u16::from_be_bytes(input[6..8].try_into()?) == 0,
            "unknown v7 WAL flags"
        );
        ensure!(
            u16::from_be_bytes(input[8..10].try_into()?) as usize == WAL_V7_HEADER_LEN,
            "invalid v7 WAL header length"
        );
        ensure!(
            u16::from_be_bytes(input[10..12].try_into()?) == 0,
            "nonzero v7 WAL reserved field"
        );
        ensure!(
            crc32fast::hash(&input[..144]) == u32::from_be_bytes(input[144..148].try_into()?),
            "v7 WAL header CRC mismatch"
        );
        ensure!(
            input[148..].iter().all(|byte| *byte == 0),
            "nonzero v7 WAL reserved bytes"
        );
        let incarnation: [u8; 16] = input[128..144].try_into()?;
        ensure!(incarnation != [0; 16], "v7 WAL incarnation is empty");

        // Reuse the v5-family identity decoder after validating the v7 envelope.
        // The projection restores its CRC slot and version without accepting v7
        // as a v5 format or changing the stored v7 bytes.
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

    pub(crate) fn digest(self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(self.encode()?).into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV7FrameKind {
    Data,
    Frontier,
}

impl WalV7FrameKind {
    fn from_wire(value: u16) -> Result<Self> {
        match value {
            WAL_V7_FRAME_KIND_DATA => Ok(Self::Data),
            WAL_V7_FRAME_KIND_FRONTIER => Ok(Self::Frontier),
            _ => anyhow::bail!("unknown v7 WAL frame kind {value}"),
        }
    }

    const fn to_wire(self) -> u16 {
        match self {
            Self::Data => WAL_V7_FRAME_KIND_DATA,
            Self::Frontier => WAL_V7_FRAME_KIND_FRONTIER,
        }
    }

    fn validate_body_len(self, body_len: usize) -> Result<()> {
        match self {
            Self::Data => ensure!(
                body_len > WAL_V7_DATA_FRAGMENT_HEADER_LEN
                    && body_len <= WAL_V7_FRAME_BODY_CAPACITY,
                "invalid v7 DATA frame body length"
            ),
            Self::Frontier => ensure!(
                body_len == WAL_V7_FRONTIER_BODY_LEN,
                "invalid v7 FRONTIER frame body length"
            ),
        }
        Ok(())
    }

    fn validate_position(
        self,
        body: &[u8],
        header_digest: &[u8; 32],
        frame_offset: u64,
    ) -> Result<()> {
        let is_generation_zero_slot = frame_offset == WAL_V7_HEADER_LEN as u64;
        match self {
            Self::Data => {
                ensure!(
                    !is_generation_zero_slot,
                    "v7 generation-zero slot must contain a FRONTIER frame"
                );
                WalV7DataFragmentHeader::decode_body(body)?;
            }
            Self::Frontier => {
                let frontier = WalV7Frontier::decode_body(body)?;
                if is_generation_zero_slot {
                    frontier.validate_generation_zero(header_digest, frame_offset)?;
                } else {
                    ensure!(
                        frontier.generation != 0,
                        "v7 generation-zero FRONTIER is not at offset 4096"
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

/// The frame-local header for one fragment of a logical v7 WAL batch.
///
/// This validates each fragment's declared index, count, total batch size, and
/// payload length. Callers assembling a batch must additionally validate that
/// fragments are contiguous, repeat the same header values, and appear in index
/// order before accepting the complete logical batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV7DataFragmentHeader {
    pub(crate) segment_ticket: u64,
    pub(crate) fragment_index: u32,
    pub(crate) fragment_count: u32,
    pub(crate) batch_bytes: u64,
}

impl WalV7DataFragmentHeader {
    /// Encodes this fragment header and payload after checking the RFC 025
    /// frame-local fragmentation rules.
    pub(crate) fn encode_body(self, payload: &[u8]) -> Result<Vec<u8>> {
        self.validate_payload_len(payload.len())?;
        let body_len = WAL_V7_DATA_FRAGMENT_HEADER_LEN
            .checked_add(payload.len())
            .ok_or_else(|| anyhow::anyhow!("v7 DATA fragment body length overflows"))?;
        ensure!(
            body_len <= WAL_V7_FRAME_BODY_CAPACITY,
            "v7 DATA fragment exceeds frame capacity"
        );

        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&self.segment_ticket.to_be_bytes());
        body.extend_from_slice(&self.fragment_index.to_be_bytes());
        body.extend_from_slice(&self.fragment_count.to_be_bytes());
        body.extend_from_slice(&self.batch_bytes.to_be_bytes());
        body.extend_from_slice(payload);
        Ok(body)
    }

    /// Decodes and validates one complete DATA fragment body, returning its
    /// header and borrowed logical-batch bytes.
    pub(crate) fn decode_body(input: &[u8]) -> Result<(Self, &[u8])> {
        ensure!(
            input.len() > WAL_V7_DATA_FRAGMENT_HEADER_LEN
                && input.len() <= WAL_V7_FRAME_BODY_CAPACITY,
            "invalid v7 DATA frame body length"
        );
        let header = Self {
            segment_ticket: u64::from_be_bytes(input[0..8].try_into()?),
            fragment_index: u32::from_be_bytes(input[8..12].try_into()?),
            fragment_count: u32::from_be_bytes(input[12..16].try_into()?),
            batch_bytes: u64::from_be_bytes(input[16..24].try_into()?),
        };
        let payload = &input[WAL_V7_DATA_FRAGMENT_HEADER_LEN..];
        header.validate_payload_len(payload.len())?;
        Ok((header, payload))
    }

    fn validate_payload_len(self, payload_len: usize) -> Result<()> {
        ensure!(self.fragment_count > 0, "v7 DATA fragment count is zero");
        ensure!(
            self.fragment_index < self.fragment_count,
            "v7 DATA fragment index is outside its batch"
        );
        ensure!(
            (WAL_V7_MIN_LOGICAL_BATCH_LEN..=WAL_V7_MAX_LOGICAL_BATCH_LEN)
                .contains(&self.batch_bytes),
            "v7 logical batch length is outside wire bounds"
        );

        let expected_count = data_fragment_count(self.batch_bytes)
            .ok_or_else(|| anyhow::anyhow!("v7 logical batch length is outside wire bounds"))?;
        ensure!(
            self.fragment_count == expected_count,
            "v7 DATA fragment count does not match logical batch length"
        );

        let fragment_capacity = WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY as u64;
        let batch_offset = u64::from(self.fragment_index)
            .checked_mul(fragment_capacity)
            .ok_or_else(|| anyhow::anyhow!("v7 DATA fragment offset overflows"))?;
        let bytes_remaining = self
            .batch_bytes
            .checked_sub(batch_offset)
            .ok_or_else(|| anyhow::anyhow!("v7 DATA fragment starts past logical batch"))?;
        let expected_payload_len = usize::try_from(bytes_remaining.min(fragment_capacity))?;
        ensure!(
            payload_len == expected_payload_len,
            "v7 DATA fragment payload length does not match its index"
        );
        Ok(())
    }
}

/// Fixed 48-byte logical batch header stored inside the DATA fragments.
///
/// The codec validates envelope fields, lengths, and CRCs. The `data` bytes are
/// the RFC 023 entry stream; callers must decode and check that stream's
/// operation count and canonical ordering before accepting a logical batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV7LogicalBatchHeader {
    pub(crate) segment_ticket: u64,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at_secs: i64,
    pub(crate) recorded_at_nanos: u32,
    pub(crate) entry_count: u32,
}

impl WalV7LogicalBatchHeader {
    /// Encodes the logical batch header, computing the data and header CRCs.
    pub(crate) fn encode(
        self,
        data: &[u8],
        limits: WalV5Limits,
    ) -> Result<[u8; WAL_V7_LOGICAL_BATCH_HEADER_LEN]> {
        validate_v7_batch_limits(limits)?;
        self.validate(data.len(), limits)?;
        let data_len = u32::try_from(data.len())?;

        let mut output = [0_u8; WAL_V7_LOGICAL_BATCH_HEADER_LEN];
        output[0..8].copy_from_slice(&self.segment_ticket.to_be_bytes());
        output[8..16].copy_from_slice(&self.commit_ts.to_be_bytes());
        output[16..24].copy_from_slice(&self.recorded_at_secs.to_be_bytes());
        output[24..28].copy_from_slice(&self.recorded_at_nanos.to_be_bytes());
        output[28..32].copy_from_slice(&self.entry_count.to_be_bytes());
        output[32..36].copy_from_slice(&data_len.to_be_bytes());
        output[36..40].copy_from_slice(&crc32fast::hash(data).to_be_bytes());
        let header_crc = crc32fast::hash(&output[..40]);
        output[40..44].copy_from_slice(&header_crc.to_be_bytes());
        Ok(output)
    }

    /// Decodes and validates one logical batch header against its exact entry
    /// stream, total length, and the ticket repeated by its DATA fragment headers.
    pub(crate) fn decode(
        input: &[u8],
        data: &[u8],
        expected_segment_ticket: u64,
        expected_batch_bytes: u64,
        limits: WalV5Limits,
    ) -> Result<Self> {
        validate_v7_batch_limits(limits)?;
        ensure!(
            input.len() == WAL_V7_LOGICAL_BATCH_HEADER_LEN,
            "invalid v7 logical batch header length"
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
            data.len() == data_len,
            "v7 logical batch data length does not match its header"
        );
        let actual_batch_bytes = (WAL_V7_LOGICAL_BATCH_HEADER_LEN as u64)
            .checked_add(u64::from(u32::try_from(data_len)?))
            .ok_or_else(|| anyhow::anyhow!("v7 logical batch length overflows"))?;
        ensure!(
            actual_batch_bytes == expected_batch_bytes,
            "v7 logical batch length does not match DATA fragments"
        );
        ensure!(
            header.segment_ticket == expected_segment_ticket,
            "v7 logical batch ticket does not match DATA fragments"
        );
        ensure!(
            u32::from_be_bytes(input[36..40].try_into()?) == crc32fast::hash(data),
            "v7 logical batch data CRC mismatch"
        );
        ensure!(
            u32::from_be_bytes(input[40..44].try_into()?) == crc32fast::hash(&input[..40]),
            "v7 logical batch header CRC mismatch"
        );
        ensure!(
            input[44..48].iter().all(|byte| *byte == 0),
            "nonzero v7 logical batch reserved field"
        );
        Ok(header)
    }

    fn validate(self, data_len: usize, limits: WalV5Limits) -> Result<()> {
        ensure!(
            self.commit_ts > 0,
            "v7 logical batch commit timestamp is zero"
        );
        ensure!(self.entry_count > 0, "v7 logical batch entry count is zero");
        ensure!(
            usize::try_from(self.entry_count)? <= limits.max_entry_count,
            "v7 logical batch entry count exceeds configured limit"
        );
        ensure!(
            data_len > 0 && data_len <= limits.max_batch_data_bytes,
            "v7 logical batch data length is outside configured limits"
        );
        ensure!(
            u32::try_from(data_len).is_ok(),
            "v7 logical batch data length exceeds wire format"
        );
        ensure!(
            self.recorded_at_nanos < 1_000_000_000,
            "v7 recorded_at nanos out of range"
        );
        Ok(())
    }
}

fn validate_v7_batch_limits(limits: WalV5Limits) -> Result<()> {
    ensure!(
        limits.max_entry_count > 0,
        "v7 entry count limit must be nonzero"
    );
    ensure!(
        limits.max_batch_data_bytes > 0,
        "v7 batch byte limit must be nonzero"
    );
    ensure!(
        limits.max_batch_data_bytes <= u32::MAX as usize,
        "v7 batch byte limit exceeds wire format"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedWalV7Frame {
    pub(crate) kind: WalV7FrameKind,
    pub(crate) header_digest: [u8; 32],
    pub(crate) frame_offset: u64,
    pub(crate) body: Vec<u8>,
}

/// Return whether an aligned frame header claims to be a FRONTIER.
///
/// This is only a discovery hint. Callers must still use
/// [`decode_frame_structural`] before accepting the frame; this predicate does
/// not validate the version, offset, binding, CRC, body, or padding.
pub(crate) fn is_frontier_frame_header(input: &[u8]) -> bool {
    input.len() >= 12
        && input[..8] == WAL_V7_FRAME_MAGIC
        && u16::from_be_bytes([input[10], input[11]]) == WAL_V7_FRAME_KIND_FRONTIER
}

fn encode_frame(
    kind: WalV7FrameKind,
    header_digest: [u8; 32],
    frame_offset: u64,
    body: &[u8],
) -> Result<[u8; WAL_V7_FRAME_LEN]> {
    validate_frame_offset(frame_offset)?;
    kind.validate_body_len(body.len())?;
    kind.validate_position(body, &header_digest, frame_offset)?;

    let mut output = [0_u8; WAL_V7_FRAME_LEN];
    output[..8].copy_from_slice(&WAL_V7_FRAME_MAGIC);
    output[8..10].copy_from_slice(&WAL_V7_FRAME_VERSION.to_be_bytes());
    output[10..12].copy_from_slice(&kind.to_wire().to_be_bytes());
    output[12..14].copy_from_slice(&(WAL_V7_FRAME_HEADER_LEN as u16).to_be_bytes());
    output[16..48].copy_from_slice(&header_digest);
    output[48..56].copy_from_slice(&frame_offset.to_be_bytes());
    output[56..60].copy_from_slice(&(body.len() as u32).to_be_bytes());
    output[WAL_V7_FRAME_HEADER_LEN..WAL_V7_FRAME_HEADER_LEN + body.len()].copy_from_slice(body);
    let crc = frame_crc(&output);
    output[60..64].copy_from_slice(&crc.to_be_bytes());

    Ok(output)
}

/// Encodes one locally valid DATA fragment frame at its assigned physical offset.
///
/// The caller must still validate the complete fragment sequence and logical
/// batch before treating a multi-frame batch as accepted.
pub(crate) fn encode_data_frame(
    header_digest: [u8; 32],
    frame_offset: u64,
    body: &[u8],
) -> Result<[u8; WAL_V7_FRAME_LEN]> {
    encode_frame(WalV7FrameKind::Data, header_digest, frame_offset, body)
}

/// Encodes the required generation-zero FRONTIER at offset 4096.
pub(crate) fn encode_generation_zero_frontier(
    header_digest: [u8; 32],
) -> Result<[u8; WAL_V7_FRAME_LEN]> {
    let frontier = WalV7Frontier {
        generation: 0,
        ticket_end: 0,
        durable_end: WAL_V7_HEADER_LEN as u64,
        last_commit_ts: 0,
        prefix_digest: header_digest,
        previous_frontier_offset: 0,
        previous_frontier_digest: [0; 32],
    };
    encode_frame(
        WalV7FrameKind::Frontier,
        header_digest,
        WAL_V7_HEADER_LEN as u64,
        &frontier.encode_body(),
    )
}

/// Encodes a nonzero FRONTIER after validating its link to the previously accepted
/// frontier. For a generation-zero predecessor, this also verifies its canonical
/// body, header binding, physical offset, and frame digest.
pub(crate) fn encode_frontier_successor(
    frontier: WalV7Frontier,
    previous: WalV7Frontier,
    previous_frame_offset: u64,
    previous_frame_digest: &[u8; 32],
    frame_offset: u64,
    header_digest: [u8; 32],
) -> Result<[u8; WAL_V7_FRAME_LEN]> {
    frontier.validate_successor(
        previous,
        previous_frame_offset,
        previous_frame_digest,
        frame_offset,
        &header_digest,
    )?;
    encode_frame(
        WalV7FrameKind::Frontier,
        header_digest,
        frame_offset,
        &frontier.encode_body(),
    )
}

/// Decodes the common envelope and frame-local invariants only.
///
/// A nonzero FRONTIER returned here is not a validated chain candidate. Use
/// [`decode_frontier_successor`] to also check its predecessor tuple and digest.
/// DATA fragment metadata and the fragment's declared payload size are checked,
/// but a caller assembling a batch must also validate the complete fragment
/// sequence and decode the reassembled logical batch.
pub(crate) fn decode_frame_structural(
    input: &[u8],
    actual_offset: u64,
    expected_header_digest: &[u8; 32],
) -> Result<DecodedWalV7Frame> {
    ensure!(
        input.len() == WAL_V7_FRAME_LEN,
        "invalid v7 WAL frame length"
    );
    validate_frame_offset(actual_offset)?;
    ensure!(
        input[..8] == WAL_V7_FRAME_MAGIC,
        "invalid v7 WAL frame magic"
    );
    ensure!(
        u16::from_be_bytes(input[8..10].try_into()?) == WAL_V7_FRAME_VERSION,
        "unsupported v7 WAL frame version"
    );
    let kind = WalV7FrameKind::from_wire(u16::from_be_bytes(input[10..12].try_into()?))?;
    ensure!(
        u16::from_be_bytes(input[12..14].try_into()?) as usize == WAL_V7_FRAME_HEADER_LEN,
        "invalid v7 WAL frame header length"
    );
    ensure!(
        u16::from_be_bytes(input[14..16].try_into()?) == 0,
        "unknown v7 WAL frame flags"
    );
    let header_digest: [u8; 32] = input[16..48].try_into()?;
    ensure!(
        &header_digest == expected_header_digest,
        "v7 WAL frame header digest mismatch"
    );
    ensure!(
        u64::from_be_bytes(input[48..56].try_into()?) == actual_offset,
        "v7 WAL frame offset mismatch"
    );
    let body_len = usize::try_from(u32::from_be_bytes(input[56..60].try_into()?))?;
    kind.validate_body_len(body_len)?;
    ensure!(
        frame_crc(input) == u32::from_be_bytes(input[60..64].try_into()?),
        "v7 WAL frame CRC mismatch"
    );
    let body_end = WAL_V7_FRAME_HEADER_LEN + body_len;
    ensure!(
        input[body_end..].iter().all(|byte| *byte == 0),
        "nonzero v7 WAL frame padding"
    );
    let body = &input[WAL_V7_FRAME_HEADER_LEN..body_end];
    kind.validate_position(body, expected_header_digest, actual_offset)?;

    Ok(DecodedWalV7Frame {
        kind,
        header_digest,
        frame_offset: actual_offset,
        body: body.to_vec(),
    })
}

/// Decodes and validates a nonzero FRONTIER against the previously accepted
/// predecessor. For a generation-zero predecessor, this also verifies its canonical
/// body, header binding, physical offset, and frame digest.
pub(crate) fn decode_frontier_successor(
    input: &[u8],
    actual_offset: u64,
    expected_header_digest: &[u8; 32],
    previous: WalV7Frontier,
    previous_frame_offset: u64,
    previous_frame_digest: &[u8; 32],
) -> Result<(WalV7Frontier, [u8; 32])> {
    let frame = decode_frame_structural(input, actual_offset, expected_header_digest)?;
    ensure!(
        frame.kind == WalV7FrameKind::Frontier,
        "v7 successor decoder received a DATA frame"
    );
    let frontier = WalV7Frontier::decode_body(&frame.body)?;
    frontier.validate_successor(
        previous,
        previous_frame_offset,
        previous_frame_digest,
        actual_offset,
        expected_header_digest,
    )?;
    let record_digest = frame_record_digest(input)?;
    Ok((frontier, record_digest))
}

pub(crate) fn frame_record_digest(frame: &[u8]) -> Result<[u8; 32]> {
    ensure!(
        frame.len() == WAL_V7_FRAME_LEN,
        "invalid v7 WAL frame length"
    );
    Ok(Sha256::digest(frame).into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV7Frontier {
    pub(crate) generation: u64,
    pub(crate) ticket_end: u64,
    pub(crate) durable_end: u64,
    pub(crate) last_commit_ts: u64,
    pub(crate) prefix_digest: [u8; 32],
    pub(crate) previous_frontier_offset: u64,
    pub(crate) previous_frontier_digest: [u8; 32],
}

impl WalV7Frontier {
    pub(crate) fn encode_body(self) -> [u8; WAL_V7_FRONTIER_BODY_LEN] {
        let mut output = [0_u8; WAL_V7_FRONTIER_BODY_LEN];
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
            input.len() == WAL_V7_FRONTIER_BODY_LEN,
            "invalid v7 FRONTIER body length"
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
        header_digest: &[u8; 32],
        frame_offset: u64,
    ) -> Result<()> {
        ensure!(
            frame_offset == WAL_V7_HEADER_LEN as u64,
            "v7 generation-zero FRONTIER is not at offset 4096"
        );
        ensure!(
            self.generation == 0
                && self.ticket_end == 0
                && self.durable_end == WAL_V7_HEADER_LEN as u64
                && self.last_commit_ts == 0
                && self.prefix_digest == *header_digest
                && self.previous_frontier_offset == 0
                && self.previous_frontier_digest == [0; 32],
            "invalid v7 generation-zero FRONTIER"
        );
        Ok(())
    }

    fn validate_successor(
        self,
        previous: Self,
        previous_frame_offset: u64,
        previous_frame_digest: &[u8; 32],
        frame_offset: u64,
        expected_header_digest: &[u8; 32],
    ) -> Result<()> {
        // Generation zero has a single canonical physical representation. Validate
        // the body, location, and digest together so a caller cannot relabel F0 as
        // residing at another aligned offset and use it to authorize a successor.
        if previous.generation == 0 {
            previous.validate_generation_zero(expected_header_digest, previous_frame_offset)?;
            let generation_zero_frame = encode_generation_zero_frontier(*expected_header_digest)?;
            ensure!(
                frame_record_digest(&generation_zero_frame)? == *previous_frame_digest,
                "v7 generation-zero FRONTIER digest mismatch"
            );
        }

        self.validate_physical_bounds(previous_frame_offset, frame_offset)?;
        let next_generation = previous
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("v7 FRONTIER generation overflows"))?;

        ensure!(
            self.generation == next_generation,
            "v7 FRONTIER generation is not consecutive"
        );
        ensure!(
            self.ticket_end > previous.ticket_end
                && self.durable_end > previous.durable_end
                && self.last_commit_ts > previous.last_commit_ts,
            "v7 FRONTIER logical boundary did not advance"
        );
        ensure!(
            self.previous_frontier_offset == previous_frame_offset
                && self.previous_frontier_digest == *previous_frame_digest,
            "v7 FRONTIER predecessor binding mismatch"
        );
        Ok(())
    }

    fn validate_physical_bounds(self, previous_frame_offset: u64, frame_offset: u64) -> Result<()> {
        validate_frame_offset(previous_frame_offset)?;
        validate_frame_offset(frame_offset)?;
        let previous_frame_end = previous_frame_offset
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("v7 previous FRONTIER offset overflows"))?;

        ensure!(
            previous_frame_offset < frame_offset
                && previous_frame_end <= self.durable_end
                && self.durable_end <= frame_offset
                && self.durable_end.is_multiple_of(WAL_V7_FRAME_LEN as u64),
            "invalid v7 FRONTIER physical bounds"
        );
        Ok(())
    }
}

fn validate_frame_offset(frame_offset: u64) -> Result<()> {
    ensure!(
        frame_offset >= WAL_V7_HEADER_LEN as u64
            && frame_offset.is_multiple_of(WAL_V7_FRAME_LEN as u64),
        "unaligned v7 WAL frame offset"
    );
    frame_offset
        .checked_add(WAL_V7_FRAME_LEN as u64)
        .ok_or_else(|| anyhow::anyhow!("v7 WAL frame end overflows"))?;
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
    use crate::pitr::{ArchiveEpochId, ChainAnchor, SegmentId, TimelineId};

    const HEADER_DIGEST_HEX: &str =
        "0120a7cbc450e3206a68a9e1923c6a0d89158b0afcdb9c34f67e9f58597f2a50";
    const FRONTIER_BODY_HEX: &str = concat!(
        "0000000000000000",
        "0000000000000000",
        "0000000000001000",
        "0000000000000000",
        "0120a7cbc450e3206a68a9e1923c6a0d89158b0afcdb9c34f67e9f58597f2a50",
        "0000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    const DATA_BODY_HEX: &str = concat!(
        "0102030405060708",
        "00000000",
        "00000001",
        "000000000000003b",
        "0102030405060708",
        "0000000000000001",
        "fffffffffffffffe",
        "1dcd6500",
        "00000001",
        "0000000b",
        "a05d100f",
        "881dce8b",
        "00000000",
        "020000000005000000016b"
    );
    const FRAGMENTED_LOGICAL_HEADER_HEX: &str = concat!(
        "0102030405060708",
        "0000000000000001",
        "fffffffffffffffe",
        "1dcd6500",
        "00000001",
        "00000faf",
        "069b7220",
        "fdd4dd5f",
        "00000000"
    );

    fn golden_header() -> WalV7Header {
        let archive_epoch_id = ArchiveEpochId([
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
            0x2e, 0x2f,
        ]);
        WalV7Header {
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
        assert!(remainder.is_empty(), "golden vector has an incomplete byte");
        pairs
            .iter()
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("golden vector is ASCII");
                u8::from_str_radix(pair, 16).expect("golden vector contains valid hex")
            })
            .collect()
    }

    fn decode_digest(value: &str) -> [u8; 32] {
        decode_hex(value)
            .try_into()
            .expect("golden digest contains exactly 32 bytes")
    }

    fn golden_frame(
        kind: u16,
        header_digest: &[u8; 32],
        frame_offset: u64,
        body: &[u8],
        crc: [u8; 4],
    ) -> [u8; WAL_V7_FRAME_LEN] {
        let mut expected = [0; WAL_V7_FRAME_LEN];
        expected[..8].copy_from_slice(b"TKVW7FR1");
        expected[8..10].copy_from_slice(&[0, 1]);
        expected[10..12].copy_from_slice(&kind.to_be_bytes());
        expected[12..14].copy_from_slice(&[0, 64]);
        expected[16..48].copy_from_slice(header_digest);
        expected[48..56].copy_from_slice(&frame_offset.to_be_bytes());
        expected[56..60].copy_from_slice(&(body.len() as u32).to_be_bytes());
        expected[60..64].copy_from_slice(&crc);
        expected[64..64 + body.len()].copy_from_slice(body);
        expected
    }

    fn raw_frame(
        kind: u16,
        header_digest: &[u8; 32],
        frame_offset: u64,
        body: &[u8],
    ) -> [u8; WAL_V7_FRAME_LEN] {
        let mut frame = golden_frame(kind, header_digest, frame_offset, body, [0; 4]);
        let crc = frame_crc(&frame);
        frame[60..64].copy_from_slice(&crc.to_be_bytes());
        frame
    }

    fn raw_data_fragment(
        segment_ticket: u64,
        fragment_index: u32,
        fragment_count: u32,
        batch_bytes: u64,
        payload_len: usize,
    ) -> Vec<u8> {
        let mut body = Vec::with_capacity(WAL_V7_DATA_FRAGMENT_HEADER_LEN + payload_len);
        body.extend_from_slice(&segment_ticket.to_be_bytes());
        body.extend_from_slice(&fragment_index.to_be_bytes());
        body.extend_from_slice(&fragment_count.to_be_bytes());
        body.extend_from_slice(&batch_bytes.to_be_bytes());
        body.resize(WAL_V7_DATA_FRAGMENT_HEADER_LEN + payload_len, 0xaa);
        body
    }

    fn recalculate_logical_batch_header_crc(header: &mut [u8]) {
        let crc = crc32fast::hash(&header[..40]);
        header[40..44].copy_from_slice(&crc.to_be_bytes());
    }

    #[test]
    fn header_and_frame_encodings_match_golden_vectors() -> Result<()> {
        let header = golden_header();
        let mut expected_header = [0; WAL_V7_HEADER_LEN];
        expected_header[..4].copy_from_slice(b"WAL2");
        expected_header[4..6].copy_from_slice(&[0, 7]);
        expected_header[8..10].copy_from_slice(&[0x10, 0]);
        expected_header[12..28].copy_from_slice(&[
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f,
        ]);
        expected_header[28..44].copy_from_slice(&[
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
            0x2e, 0x2f,
        ]);
        expected_header[44..52].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        expected_header[128..144].copy_from_slice(&[
            0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d,
            0x3e, 0x3f,
        ]);
        expected_header[144..148].copy_from_slice(&[0x8f, 0xde, 0xea, 0x31]);
        assert_eq!(header.encode()?, expected_header);
        assert_eq!(header.digest()?, decode_digest(HEADER_DIGEST_HEX));
        assert_eq!(WalV7Header::decode(&expected_header)?, header);

        let header_digest = decode_digest(HEADER_DIGEST_HEX);
        let frontier = WalV7Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let frontier_body = decode_hex(FRONTIER_BODY_HEX);
        assert_eq!(frontier.encode_body().as_slice(), frontier_body);
        assert_eq!(WalV7Frontier::decode_body(&frontier_body)?, frontier);

        let frontier_frame = encode_generation_zero_frontier(header_digest)?;
        let expected_frontier_frame = golden_frame(
            2,
            &header_digest,
            4096,
            &frontier_body,
            [0x78, 0x87, 0x9f, 0x53],
        );
        assert_eq!(frontier_frame, expected_frontier_frame);
        assert_eq!(
            frame_record_digest(&frontier_frame)?,
            decode_digest("dff5d16a32f20befd3504467ca92573d98fcd4dbe2b7b107219c121d2ce1bb52")
        );

        let data_body = decode_hex(DATA_BODY_HEX);
        let data_frame = encode_data_frame(header_digest, 8192, &data_body)?;
        let expected_data_frame = golden_frame(
            1,
            &header_digest,
            8192,
            &data_body,
            [0x02, 0xa5, 0xe1, 0xb3],
        );
        assert_eq!(data_frame, expected_data_frame);
        assert_eq!(
            frame_record_digest(&data_frame)?,
            decode_digest("1e41de6ef479ae926ef18c27b83bef5167bf0a48e99de41bc9349ca3c9db3e29")
        );
        let (fragment_header, batch) = WalV7DataFragmentHeader::decode_body(&data_body)?;
        assert_eq!(
            fragment_header,
            WalV7DataFragmentHeader {
                segment_ticket: 0x0102_0304_0506_0708,
                fragment_index: 0,
                fragment_count: 1,
                batch_bytes: 59,
            }
        );
        assert_eq!(batch.len(), 59);
        let batch_header = WalV7LogicalBatchHeader {
            segment_ticket: 0x0102_0304_0506_0708,
            commit_ts: 1,
            recorded_at_secs: -2,
            recorded_at_nanos: 500_000_000,
            entry_count: 1,
        };
        let logical_header = &batch[..WAL_V7_LOGICAL_BATCH_HEADER_LEN];
        let entry_stream = &batch[WAL_V7_LOGICAL_BATCH_HEADER_LEN..];
        assert_eq!(
            batch_header
                .encode(entry_stream, crate::pitr::LIVE_WAL_V5_LIMITS)?
                .as_slice(),
            logical_header
        );
        assert_eq!(
            WalV7LogicalBatchHeader::decode(
                logical_header,
                entry_stream,
                fragment_header.segment_ticket,
                fragment_header.batch_bytes,
                crate::pitr::LIVE_WAL_V5_LIMITS,
            )?,
            batch_header
        );
        assert_eq!(
            decode_frame_structural(&frontier_frame, 4096, &header_digest)?.body,
            frontier_body
        );
        assert_eq!(
            decode_frame_structural(&data_frame, 8192, &header_digest)?.body,
            data_body
        );
        Ok(())
    }

    #[test]
    fn fragmented_logical_batch_matches_golden_frames() -> Result<()> {
        let header_digest = decode_digest(HEADER_DIGEST_HEX);
        let batch_header = WalV7LogicalBatchHeader {
            segment_ticket: 0x0102_0304_0506_0708,
            commit_ts: 1,
            recorded_at_secs: -2,
            recorded_at_nanos: 500_000_000,
            entry_count: 1,
        };
        let mut entry_stream = Vec::with_capacity(4015);
        entry_stream.extend_from_slice(&[1, 0]);
        entry_stream.extend_from_slice(&4009_u32.to_be_bytes());
        entry_stream.extend_from_slice(&1_u32.to_be_bytes());
        entry_stream.push(b'k');
        entry_stream.extend_from_slice(&4000_u32.to_be_bytes());
        entry_stream.resize(4015, 0x5a);

        let expected_logical_header = decode_hex(FRAGMENTED_LOGICAL_HEADER_HEX);
        assert_eq!(
            expected_logical_header.len(),
            WAL_V7_LOGICAL_BATCH_HEADER_LEN
        );
        assert_eq!(
            batch_header
                .encode(&entry_stream, crate::pitr::LIVE_WAL_V5_LIMITS)?
                .as_slice(),
            expected_logical_header
        );

        let mut expected_batch = expected_logical_header;
        expected_batch.extend_from_slice(&entry_stream);
        assert_eq!(expected_batch.len(), 4063);

        let mut frames = Vec::with_capacity(2);
        let frame_crcs = [[0x88, 0xa8, 0x04, 0xbb], [0x23, 0x33, 0x4c, 0x3a]];
        let frame_digests = [
            "a7ffa98104cf11f5d8140c4bbdb722191aaedb7715873d376619d2a375ed38ae",
            "5def83754ab4a1267c0b98b61182aa4fb1c7ded0a0469c6b0d85db48746f52e4",
        ];
        for fragment_index in 0..2_u32 {
            let payload_start =
                usize::try_from(fragment_index)? * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY;
            let payload_end =
                (payload_start + WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY).min(expected_batch.len());
            let payload = &expected_batch[payload_start..payload_end];
            let fragment_header = WalV7DataFragmentHeader {
                segment_ticket: batch_header.segment_ticket,
                fragment_index,
                fragment_count: 2,
                batch_bytes: expected_batch.len() as u64,
            };
            let actual_body = fragment_header.encode_body(payload)?;

            let mut expected_body = decode_hex("010203040506070800000000000000020000000000000fdf");
            expected_body[8..12].copy_from_slice(&fragment_index.to_be_bytes());
            expected_body.extend_from_slice(payload);
            assert_eq!(actual_body, expected_body);

            let frame_offset = 8192 + u64::from(fragment_index) * WAL_V7_FRAME_LEN as u64;
            let frame = encode_data_frame(header_digest, frame_offset, &actual_body)?;
            assert_eq!(
                frame,
                golden_frame(
                    1,
                    &header_digest,
                    frame_offset,
                    &expected_body,
                    frame_crcs[usize::try_from(fragment_index)?],
                )
            );
            assert_eq!(
                frame_record_digest(&frame)?,
                decode_digest(frame_digests[usize::try_from(fragment_index)?])
            );
            frames.push(frame);
        }

        let mut reassembled_batch = Vec::with_capacity(expected_batch.len());
        for (fragment_index, frame) in frames.iter().enumerate() {
            let frame_offset = 8192 + fragment_index as u64 * WAL_V7_FRAME_LEN as u64;
            let decoded_frame = decode_frame_structural(frame, frame_offset, &header_digest)?;
            let (fragment_header, payload) =
                WalV7DataFragmentHeader::decode_body(&decoded_frame.body)?;
            assert_eq!(fragment_header.fragment_index, fragment_index as u32);
            assert_eq!(fragment_header.fragment_count, 2);
            assert_eq!(fragment_header.batch_bytes, expected_batch.len() as u64);
            reassembled_batch.extend_from_slice(payload);
        }
        assert_eq!(reassembled_batch, expected_batch);
        let (logical_header, data) = reassembled_batch.split_at(WAL_V7_LOGICAL_BATCH_HEADER_LEN);
        assert_eq!(
            WalV7LogicalBatchHeader::decode(
                logical_header,
                data,
                batch_header.segment_ticket,
                expected_batch.len() as u64,
                crate::pitr::LIVE_WAL_V5_LIMITS,
            )?,
            batch_header
        );
        Ok(())
    }

    #[test]
    fn generation_zero_frontier_owns_the_first_frame_slot() -> Result<()> {
        let header_digest = decode_digest(HEADER_DIGEST_HEX);
        let data_body = decode_hex(DATA_BODY_HEX);

        assert!(encode_data_frame(header_digest, 4096, &data_body).is_err());

        let mut misplaced_data = encode_data_frame(header_digest, 8192, &data_body)?;
        misplaced_data[48..56].copy_from_slice(&4096_u64.to_be_bytes());
        let misplaced_data_crc = frame_crc(&misplaced_data);
        misplaced_data[60..64].copy_from_slice(&misplaced_data_crc.to_be_bytes());
        assert!(decode_frame_structural(&misplaced_data, 4096, &header_digest).is_err());

        let mut misplaced_frontier = encode_generation_zero_frontier(header_digest)?;
        misplaced_frontier[48..56].copy_from_slice(&8192_u64.to_be_bytes());
        let misplaced_frontier_crc = frame_crc(&misplaced_frontier);
        misplaced_frontier[60..64].copy_from_slice(&misplaced_frontier_crc.to_be_bytes());
        assert!(decode_frame_structural(&misplaced_frontier, 8192, &header_digest).is_err());

        Ok(())
    }

    #[test]
    fn data_fragment_count_matches_wire_boundaries() {
        // Fixed RFC 025 sizes keep these expectations independent of the
        // production ceiling calculation and framing constants.
        for (batch_bytes, expected_count) in [
            (49, 1),
            (4008, 1),
            (4009, 2),
            (8016, 2),
            (8017, 3),
            (4_294_967_343, 1_071_599),
        ] {
            assert_eq!(data_fragment_count(batch_bytes), Some(expected_count));
        }
        for batch_bytes in [0, 48, 4_294_967_344, u64::MAX] {
            assert_eq!(data_fragment_count(batch_bytes), None);
        }
    }

    #[test]
    fn data_fragment_shape_is_validated_for_encode_and_decode() -> Result<()> {
        let header_digest = decode_digest(HEADER_DIGEST_HEX);
        let fragment_capacity = WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY;
        let first_fragment = WalV7DataFragmentHeader {
            segment_ticket: 7,
            fragment_index: 0,
            fragment_count: 2,
            batch_bytes: (fragment_capacity + 1) as u64,
        }
        .encode_body(&vec![0xaa; fragment_capacity])?;
        let final_fragment = WalV7DataFragmentHeader {
            segment_ticket: 7,
            fragment_index: 1,
            fragment_count: 2,
            batch_bytes: (fragment_capacity + 1) as u64,
        }
        .encode_body(&[0xbb])?;
        assert_eq!(first_fragment.len(), WAL_V7_FRAME_BODY_CAPACITY);
        assert_eq!(final_fragment.len(), WAL_V7_DATA_FRAGMENT_HEADER_LEN + 1);
        assert!(encode_data_frame(header_digest, 8192, &first_fragment).is_ok());
        assert!(encode_data_frame(header_digest, 12288, &final_fragment).is_ok());

        let malformed_fragments = [
            raw_data_fragment(1, 0, 0, 49, 49),
            raw_data_fragment(1, 1, 1, 49, 49),
            raw_data_fragment(1, 0, 2, 49, 49),
            raw_data_fragment(1, 0, 1, 49, 48),
            raw_data_fragment(
                1,
                0,
                2,
                (fragment_capacity + 1) as u64,
                fragment_capacity - 1,
            ),
            raw_data_fragment(1, 1, 2, (fragment_capacity + 1) as u64, 2),
            raw_data_fragment(1, 0, 1, 48, 48),
            raw_data_fragment(1, 0, 1, u64::from(u32::MAX) + 50, 1),
        ];
        let frame_offset = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64;
        for body in malformed_fragments {
            assert!(encode_data_frame(header_digest, frame_offset, &body).is_err());
            let malformed_frame =
                raw_frame(WAL_V7_FRAME_KIND_DATA, &header_digest, frame_offset, &body);
            assert!(
                decode_frame_structural(&malformed_frame, frame_offset, &header_digest).is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn logical_batch_header_checks_ticket_limits_timestamps_and_crcs() -> Result<()> {
        let entry_stream = decode_hex("020000000005000000016b");
        let header = WalV7LogicalBatchHeader {
            segment_ticket: 17,
            commit_ts: 29,
            recorded_at_secs: -2,
            recorded_at_nanos: 500_000_000,
            entry_count: 1,
        };
        let limits = crate::pitr::LIVE_WAL_V5_LIMITS;
        let encoded = header.encode(&entry_stream, limits)?;
        let expected_batch_bytes =
            WAL_V7_LOGICAL_BATCH_HEADER_LEN as u64 + entry_stream.len() as u64;
        assert_eq!(
            WalV7LogicalBatchHeader::decode(
                &encoded,
                &entry_stream,
                17,
                expected_batch_bytes,
                limits,
            )?,
            header
        );
        assert!(
            WalV7LogicalBatchHeader::decode(
                &encoded,
                &entry_stream,
                18,
                expected_batch_bytes,
                limits,
            )
            .is_err()
        );
        assert!(
            WalV7LogicalBatchHeader::decode(
                &encoded,
                &entry_stream,
                17,
                expected_batch_bytes + 1,
                limits,
            )
            .is_err()
        );

        let mut corrupted_data = entry_stream.clone();
        corrupted_data[10] ^= 1;
        assert!(
            WalV7LogicalBatchHeader::decode(
                &encoded,
                &corrupted_data,
                17,
                expected_batch_bytes,
                limits,
            )
            .is_err()
        );

        let mut corrupted_header = encoded;
        corrupted_header[8] ^= 1;
        assert!(
            WalV7LogicalBatchHeader::decode(
                &corrupted_header,
                &entry_stream,
                17,
                expected_batch_bytes,
                limits,
            )
            .is_err()
        );

        let mut nonzero_reserved = encoded;
        nonzero_reserved[47] = 1;
        assert!(
            WalV7LogicalBatchHeader::decode(
                &nonzero_reserved,
                &entry_stream,
                17,
                expected_batch_bytes,
                limits,
            )
            .is_err()
        );

        let mut invalid_nanos = encoded;
        invalid_nanos[24..28].copy_from_slice(&1_000_000_000_u32.to_be_bytes());
        recalculate_logical_batch_header_crc(&mut invalid_nanos);
        assert!(
            WalV7LogicalBatchHeader::decode(
                &invalid_nanos,
                &entry_stream,
                17,
                expected_batch_bytes,
                limits,
            )
            .is_err()
        );

        let mut mismatched_data_len = encoded;
        mismatched_data_len[32..36]
            .copy_from_slice(&(u32::try_from(entry_stream.len())? - 1).to_be_bytes());
        recalculate_logical_batch_header_crc(&mut mismatched_data_len);
        assert!(
            WalV7LogicalBatchHeader::decode(
                &mismatched_data_len,
                &entry_stream,
                17,
                expected_batch_bytes,
                limits,
            )
            .is_err()
        );

        let small_limits = crate::pitr::WalV5Limits {
            max_batch_data_bytes: entry_stream.len() - 1,
            ..limits
        };
        assert!(header.encode(&entry_stream, small_limits).is_err());
        Ok(())
    }

    #[test]
    fn nonzero_frontier_must_fit_its_physical_prefix() -> Result<()> {
        let header_digest = decode_digest(HEADER_DIGEST_HEX);
        let frontier = WalV7Frontier {
            generation: 1,
            ticket_end: 1,
            durable_end: 1 << 20,
            last_commit_ts: 1,
            prefix_digest: [0x44; 32],
            previous_frontier_offset: WAL_V7_HEADER_LEN as u64,
            previous_frontier_digest: [0x55; 32],
        };
        let body = frontier.encode_body();
        let previous = WalV7Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let previous_frame = encode_generation_zero_frontier(header_digest)?;
        let previous_frame_digest = frame_record_digest(&previous_frame)?;
        assert!(
            encode_frontier_successor(
                frontier,
                previous,
                WAL_V7_HEADER_LEN as u64,
                &previous_frame_digest,
                (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64,
                header_digest,
            )
            .is_err()
        );

        let malformed = raw_frame(
            WAL_V7_FRAME_KIND_FRONTIER,
            &header_digest,
            (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64,
            &body,
        );
        assert!(
            decode_frame_structural(
                &malformed,
                (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64,
                &header_digest,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn generation_zero_predecessor_must_be_canonical() -> Result<()> {
        let header_digest = decode_digest(HEADER_DIGEST_HEX);
        let generation_zero = WalV7Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let generation_zero_frame = encode_generation_zero_frontier(header_digest)?;
        let generation_zero_digest = frame_record_digest(&generation_zero_frame)?;
        let candidate_offset = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN * 3) as u64;
        let candidate = WalV7Frontier {
            generation: 1,
            ticket_end: 1,
            durable_end: (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN * 2) as u64,
            last_commit_ts: 1,
            prefix_digest: [0x44; 32],
            previous_frontier_offset: (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64,
            previous_frontier_digest: generation_zero_digest,
        };
        let candidate_frame = raw_frame(
            WAL_V7_FRAME_KIND_FRONTIER,
            &header_digest,
            candidate_offset,
            &candidate.encode_body(),
        );

        // The body and digest are canonical F0 values, but the supplied offset
        // attempts to relocate F0 to the next frame slot.
        let relocated_offset = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64;
        assert!(
            encode_frontier_successor(
                candidate,
                generation_zero,
                relocated_offset,
                &generation_zero_digest,
                candidate_offset,
                header_digest,
            )
            .is_err()
        );
        assert!(
            decode_frontier_successor(
                &candidate_frame,
                candidate_offset,
                &header_digest,
                generation_zero,
                relocated_offset,
                &generation_zero_digest,
            )
            .is_err()
        );

        let wrong_header_binding = WalV7Frontier {
            prefix_digest: [0x99; 32],
            ..generation_zero
        };
        assert!(
            encode_frontier_successor(
                candidate,
                wrong_header_binding,
                WAL_V7_HEADER_LEN as u64,
                &generation_zero_digest,
                candidate_offset,
                header_digest,
            )
            .is_err()
        );
        assert!(
            decode_frontier_successor(
                &candidate_frame,
                candidate_offset,
                &header_digest,
                wrong_header_binding,
                WAL_V7_HEADER_LEN as u64,
                &generation_zero_digest,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn successor_codec_requires_monotonic_predecessor_fields() -> Result<()> {
        let header_digest = decode_digest(HEADER_DIGEST_HEX);
        let previous = WalV7Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let previous_frame = encode_generation_zero_frontier(header_digest)?;
        let previous_frame_digest = frame_record_digest(&previous_frame)?;
        let frame_offset = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN * 2) as u64;
        let candidate = WalV7Frontier {
            generation: 1,
            ticket_end: 0,
            durable_end: (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64,
            last_commit_ts: 1,
            prefix_digest: [0x66; 32],
            previous_frontier_offset: WAL_V7_HEADER_LEN as u64,
            previous_frontier_digest: previous_frame_digest,
        };
        let candidate_frame = raw_frame(
            WAL_V7_FRAME_KIND_FRONTIER,
            &header_digest,
            frame_offset,
            &candidate.encode_body(),
        );
        assert!(decode_frame_structural(&candidate_frame, frame_offset, &header_digest).is_ok());
        assert!(
            encode_frontier_successor(
                candidate,
                previous,
                WAL_V7_HEADER_LEN as u64,
                &previous_frame_digest,
                frame_offset,
                header_digest,
            )
            .is_err()
        );
        assert!(
            decode_frontier_successor(
                &candidate_frame,
                frame_offset,
                &header_digest,
                previous,
                WAL_V7_HEADER_LEN as u64,
                &previous_frame_digest,
            )
            .is_err()
        );

        let valid_candidate = WalV7Frontier {
            ticket_end: 1,
            durable_end: frame_offset,
            last_commit_ts: 1,
            ..candidate
        };
        let valid_candidate_frame = encode_frontier_successor(
            valid_candidate,
            previous,
            WAL_V7_HEADER_LEN as u64,
            &previous_frame_digest,
            frame_offset,
            header_digest,
        )?;
        let (decoded_candidate, decoded_digest) = decode_frontier_successor(
            &valid_candidate_frame,
            frame_offset,
            &header_digest,
            previous,
            WAL_V7_HEADER_LEN as u64,
            &previous_frame_digest,
        )?;
        assert_eq!(decoded_candidate, valid_candidate);
        assert_eq!(decoded_digest, frame_record_digest(&valid_candidate_frame)?);
        Ok(())
    }
}
