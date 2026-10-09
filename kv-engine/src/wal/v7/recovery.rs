//! Bounded v7 WAL frontier discovery and candidate-prefix verification.
//!
//! Discovery is deliberately independent of DATA decoding. Its results are
//! structural candidates only; recovery must still verify each candidate's
//! covered prefix, frontier chain, authority, and durable anchors before it can
//! select or install a boundary.

use std::io::{Read, Seek, SeekFrom};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use super::codec::{
    DecodedWalV7Frame, WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WAL_V7_LOGICAL_BATCH_HEADER_LEN,
    WalV7DataFragmentHeader, WalV7FrameKind, WalV7Frontier, WalV7Header, WalV7LogicalBatchHeader,
    decode_frame_structural, decode_frontier_successor, encode_frontier_successor,
    encode_generation_zero_frontier, frame_record_digest, is_frontier_frame_header,
};
use crate::pitr::{
    ChainAnchor, LIVE_WAL_V5_LIMITS, RecordedAt, decode_wal_entry_stream,
    manifest::{ActiveBoundary, ImmutableBoundary},
};
use crate::wal::MAX_WAL_FILE_SIZE;

const DISCOVERY_BATCH_FRAMES: usize = 64;
const MAX_REJECTED_FRONTIER_DIAGNOSTICS: usize = 64;
const MAX_REJECTION_REASON_CHARS: usize = 192;
const IMMUTABLE_HASH_BUFFER_LEN: usize = 64 * 1024;

const FRAME_LEN_U64: u64 = WAL_V7_FRAME_LEN as u64;
const HEADER_LEN_U64: u64 = WAL_V7_HEADER_LEN as u64;

/// A structurally valid FRONTIER frame discovered in the physical WAL image.
///
/// The predecessor link and the DATA/control prefix are not verified by
/// discovery. Keep this distinct from an installed or recovered boundary so a
/// caller cannot accidentally treat the newest marker as authoritative.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalV7FrontierCandidate {
    pub(crate) frame_offset: u64,
    pub(crate) frontier: WalV7Frontier,
    pub(crate) frame_digest: [u8; 32],
}

/// Bounded diagnostic for a frame whose aligned header claims FRONTIER but
/// whose structural validation fails.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalV7RejectedFrontier {
    pub(crate) frame_offset: u64,
    pub(crate) reason: String,
}

/// Results from scanning complete aligned frame slots in descending physical
/// order. Rejected diagnostics are capped; the total count is retained.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalV7FrontierDiscovery {
    /// Structural candidates in descending `frame_offset` order.
    pub(crate) candidates: Vec<WalV7FrontierCandidate>,
    /// The first rejected candidates encountered from the physical tail.
    pub(crate) rejected_frontiers: Vec<WalV7RejectedFrontier>,
    pub(crate) rejected_frontier_count: u64,
    pub(crate) scanned_frame_count: u64,
    /// Bytes after the last complete 4096-byte frame. They are not candidates.
    pub(crate) incomplete_tail_bytes: u64,
}

impl WalV7FrontierDiscovery {
    fn accept(&mut self, candidate: WalV7FrontierCandidate) -> Result<()> {
        self.candidates
            .try_reserve(1)
            .context("reserve v7 FRONTIER candidate metadata")?;
        self.candidates.push(candidate);

        Ok(())
    }

    fn reject(&mut self, frame_offset: u64, error: anyhow::Error) -> Result<()> {
        self.rejected_frontier_count = self
            .rejected_frontier_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("v7 rejected-frontier count overflows"))?;
        if self.rejected_frontiers.len() < MAX_REJECTED_FRONTIER_DIAGNOSTICS {
            let reason = error
                .to_string()
                .chars()
                .take(MAX_REJECTION_REASON_CHARS)
                .collect();
            self.rejected_frontiers.push(WalV7RejectedFrontier {
                frame_offset,
                reason,
            });
        }

        Ok(())
    }
}

/// Candidate prefix verification results from one bounded forward pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalV7FrontierPrefixVerification {
    /// Candidates whose exact DATA and control prefixes verified.
    pub(crate) verified_candidates: Vec<WalV7FrontierCandidate>,
    /// A bounded sample of candidates rejected during prefix verification.
    pub(crate) rejected_candidates: Vec<WalV7RejectedFrontier>,
    pub(crate) rejected_candidate_count: u64,
    /// The end of the forward prefix verified before corruption stopped scanning.
    pub(crate) verified_prefix_end: u64,
    /// The first invalid physical frame, if scanning stopped before the longest
    /// candidate prefix requested.
    pub(crate) stopped_at: Option<WalV7RejectedFrontier>,
}

/// Durable authority supplied by the source manifest or a verified backup/catalog.
/// A sidecar discovered beside an Active WAL must never be used to construct an
/// immutable variant of this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalV7RecoveryAuthority {
    Active {
        predecessor: ChainAnchor,
        durable_anchors: Vec<ActiveBoundary>,
    },
    Sealing {
        predecessor: ChainAnchor,
        boundary: ImmutableBoundary,
    },
    Sealed {
        predecessor: ChainAnchor,
        boundary: ImmutableBoundary,
    },
}

/// Bounded selection diagnostics. The physical tail is the range an Active
/// fresh-inode normalization will replace; immutable recovery never discards
/// bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalV7RecoveryDiagnostics {
    pub(crate) selected_frame_offset: u64,
    pub(crate) rejected_frontiers: Vec<WalV7RejectedFrontier>,
    pub(crate) rejected_frontier_count: u64,
    pub(crate) discarded_physical_tail: Option<(u64, u64)>,
    pub(crate) fallback_reason: Option<String>,
}

/// Selected logical boundary plus the physical certificate that proved it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalV7RecoverySelection {
    pub(crate) candidate: WalV7FrontierCandidate,
    pub(crate) active_boundary: ActiveBoundary,
    pub(crate) diagnostics: WalV7RecoveryDiagnostics,
}

impl WalV7FrontierPrefixVerification {
    fn reject(&mut self, frame_offset: u64, error: impl std::fmt::Display) -> Result<()> {
        self.rejected_candidate_count = self
            .rejected_candidate_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("v7 rejected-candidate count overflows"))?;
        if self.rejected_candidates.len() < MAX_REJECTED_FRONTIER_DIAGNOSTICS {
            self.rejected_candidates
                .try_reserve(1)
                .context("reserve v7 rejected-candidate diagnostic")?;
            self.rejected_candidates.push(WalV7RejectedFrontier {
                frame_offset,
                reason: bounded_reason(error),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct WalV7PrefixCheckpoint {
    /// DATA extent for this ticket, or 4096 for the empty prefix.
    data_start: u64,
    data_end: u64,
    last_commit_ts: u64,
    prefix_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug)]
struct WalV7PrefixFrontier {
    frame_offset: u64,
    frontier: WalV7Frontier,
    frame_digest: [u8; 32],
    chain_breaks_through: u64,
}

/// Constant-size coverage constraints; every anchor still needs exact prefix
/// validation before these constraints can authorize a candidate.
struct WalV7ActiveAnchorRequirements {
    maximum_ticket_anchor: ActiveBoundary,
    maximum_durable_end: u64,
    maximum_last_commit_ts: u64,
    lower_ticket_floor: Option<(u64, u64)>,
    maximum_ticket_anchors_agree: bool,
}

impl WalV7ActiveAnchorRequirements {
    fn new(anchors: &[ActiveBoundary]) -> Result<Self> {
        let maximum_ticket_anchor = anchors
            .iter()
            .max_by_key(|anchor| anchor.ticket_end)
            .copied()
            .context("Active v7 recovery requires at least one durable anchor")?;
        let mut requirements = Self {
            maximum_ticket_anchor,
            maximum_durable_end: maximum_ticket_anchor.durable_end,
            maximum_last_commit_ts: maximum_ticket_anchor.last_commit_ts,
            lower_ticket_floor: None,
            maximum_ticket_anchors_agree: true,
        };
        for anchor in anchors {
            requirements.maximum_durable_end =
                requirements.maximum_durable_end.max(anchor.durable_end);
            requirements.maximum_last_commit_ts = requirements
                .maximum_last_commit_ts
                .max(anchor.last_commit_ts);
            if anchor.ticket_end == maximum_ticket_anchor.ticket_end {
                requirements.maximum_ticket_anchors_agree &= anchor.durable_end
                    == maximum_ticket_anchor.durable_end
                    && anchor.last_commit_ts == maximum_ticket_anchor.last_commit_ts
                    && anchor.prefix_digest == maximum_ticket_anchor.prefix_digest;
            } else {
                let (end, timestamp) = requirements.lower_ticket_floor.unwrap_or((0, 0));
                requirements.lower_ticket_floor = Some((
                    end.max(anchor.durable_end),
                    timestamp.max(anchor.last_commit_ts),
                ));
            }
        }
        Ok(requirements)
    }

    fn covers(&self, candidate: &WalV7FrontierCandidate) -> bool {
        if !candidate_covers_anchor(candidate, &self.maximum_ticket_anchor) {
            return false;
        }
        if candidate.frontier.ticket_end > self.maximum_ticket_anchor.ticket_end {
            return candidate.frontier.durable_end > self.maximum_durable_end
                && candidate.frontier.last_commit_ts > self.maximum_last_commit_ts;
        }

        // Same-ticket anchors can name different physical ends when an end
        // includes a trailing control frame. A higher-ticket candidate can
        // cover both, but an equal-ticket candidate must match every tuple.
        self.maximum_ticket_anchors_agree
            && self.lower_ticket_floor.is_none_or(|(end, timestamp)| {
                candidate.frontier.durable_end > end
                    && candidate.frontier.last_commit_ts > timestamp
            })
    }
}

fn bounded_reason(error: impl std::fmt::Display) -> String {
    error
        .to_string()
        .chars()
        .take(MAX_REJECTION_REASON_CHARS)
        .collect()
}

/// Scan aligned frame slots backward and collect structurally valid FRONTIERs.
///
/// Reads are issued in bounded batches and continue across zero-filled holes,
/// invalid DATA, preallocated regions, and a partial final frame. Actual read or
/// seek failures are returned to the caller because they do not prove that a
/// candidate is absent. `file_len` must be a length snapshot taken while the
/// caller has exclusive recovery ownership of the WAL. The RFC's 1 GiB hard
/// WAL limit is enforced here, which also bounds the maximum candidate count.
///
/// This function validates each marker's frame envelope, header binding, CRC,
/// canonical padding, body, physical bounds, and generation-zero placement. It
/// does not follow predecessor pointers or verify logical batches.
pub(crate) fn discover_frontier_candidates<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    header_digest: &[u8; 32],
) -> Result<WalV7FrontierDiscovery> {
    ensure!(
        file_len >= HEADER_LEN_U64,
        "v7 WAL image is shorter than its immutable header"
    );
    ensure!(
        file_len <= MAX_WAL_FILE_SIZE,
        "v7 WAL image exceeds the 1 GiB recovery limit"
    );

    let complete_end = file_len - file_len % FRAME_LEN_U64;
    let mut discovery = WalV7FrontierDiscovery {
        incomplete_tail_bytes: file_len - complete_end,
        ..WalV7FrontierDiscovery::default()
    };
    let Some(mut batch_last_offset) = complete_end.checked_sub(FRAME_LEN_U64) else {
        return Ok(discovery);
    };
    if batch_last_offset < HEADER_LEN_U64 {
        return Ok(discovery);
    }

    loop {
        let available_frames = (batch_last_offset - HEADER_LEN_U64) / FRAME_LEN_U64 + 1;
        let batch_frames = available_frames.min(DISCOVERY_BATCH_FRAMES as u64);
        let batch_bytes = batch_frames
            .checked_mul(FRAME_LEN_U64)
            .ok_or_else(|| anyhow::anyhow!("v7 discovery batch length overflows"))?;
        let batch_first_offset = batch_last_offset
            .checked_add(FRAME_LEN_U64)
            .and_then(|batch_end| batch_end.checked_sub(batch_bytes))
            .ok_or_else(|| anyhow::anyhow!("v7 discovery batch offset overflows"))?;
        let batch_bytes = usize::try_from(batch_bytes)?;
        let mut batch = vec![0_u8; batch_bytes];

        reader
            .seek(SeekFrom::Start(batch_first_offset))
            .with_context(|| format!("seek v7 WAL to {batch_first_offset}"))?;
        reader
            .read_exact(&mut batch)
            .with_context(|| format!("read v7 WAL discovery batch at {batch_first_offset}"))?;

        let batch_frames = usize::try_from(batch_frames)?;
        for frame_index in (0..batch_frames).rev() {
            discovery.scanned_frame_count = discovery
                .scanned_frame_count
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("v7 scanned-frame count overflows"))?;

            let frame_start = frame_index * WAL_V7_FRAME_LEN;
            let frame_bytes = &batch[frame_start..frame_start + WAL_V7_FRAME_LEN];
            if !is_frontier_frame_header(frame_bytes) {
                continue;
            }

            let frame_offset = batch_first_offset
                .checked_add(u64::try_from(frame_start)?)
                .ok_or_else(|| anyhow::anyhow!("v7 discovered frame offset overflows"))?;
            match decode_frame_structural(frame_bytes, frame_offset, header_digest) {
                Ok(frame) => {
                    if let Some(candidate) = frontier_candidate(frame, frame_bytes)? {
                        discovery.accept(candidate)?;
                    }
                }
                Err(error) => discovery.reject(frame_offset, error)?,
            }
        }

        if batch_first_offset == HEADER_LEN_U64 {
            break;
        }
        batch_last_offset = batch_first_offset
            .checked_sub(FRAME_LEN_U64)
            .ok_or_else(|| anyhow::anyhow!("v7 discovery frame offset underflows"))?;
    }

    Ok(discovery)
}

/// Verify every discovered candidate using a single bounded forward pass.
///
/// The caller must hold exclusive recovery ownership of the image between
/// discovery and verification. `candidates` must come from discovery on this
/// same unchanged image, and `file_len` must be that same stable length snapshot.
/// Structural candidates beyond a corrupt physical prefix can still verify if
/// their named `durable_end` excludes the corrupt bytes.
pub(crate) fn verify_frontier_candidate_prefixes<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    candidates: &[WalV7FrontierCandidate],
) -> Result<WalV7FrontierPrefixVerification> {
    verify_frontier_candidate_prefixes_with_anchors(reader, file_len, candidates, &[], None)
}

fn verify_frontier_candidate_prefixes_with_anchors<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    candidates: &[WalV7FrontierCandidate],
    active_anchors: &[ActiveBoundary],
    expected_predecessor: Option<ChainAnchor>,
) -> Result<WalV7FrontierPrefixVerification> {
    ensure!(
        file_len >= HEADER_LEN_U64 + FRAME_LEN_U64,
        "installed v7 WAL image is missing generation-zero FRONTIER"
    );
    ensure!(
        file_len <= MAX_WAL_FILE_SIZE,
        "v7 WAL image exceeds the 1 GiB recovery limit"
    );

    reader
        .seek(SeekFrom::Start(0))
        .context("seek v7 WAL immutable header")?;
    let mut header_bytes = [0_u8; WAL_V7_HEADER_LEN];
    reader
        .read_exact(&mut header_bytes)
        .context("read v7 WAL immutable header")?;
    let header = WalV7Header::decode(&header_bytes)?;
    ensure!(
        header.encode()? == header_bytes,
        "v7 WAL immutable header is not canonical"
    );
    if let Some(expected_predecessor) = expected_predecessor {
        ensure!(
            header.predecessor == expected_predecessor,
            "v7 WAL predecessor differs from authoritative segment identity"
        );
    }
    let header_digest: [u8; 32] = Sha256::digest(header_bytes).into();
    for anchor in active_anchors {
        validate_anchor_identity(anchor, header)?;
        ensure!(
            anchor.durable_end <= file_len,
            "Active v7 durable anchor exceeds physical WAL length"
        );
    }

    let mut verification = WalV7FrontierPrefixVerification::default();
    let generation_zero_bytes = read_frame(reader, HEADER_LEN_U64)?;
    let generation_zero =
        decode_frame_structural(&generation_zero_bytes, HEADER_LEN_U64, &header_digest)?;
    ensure!(
        generation_zero.kind == WalV7FrameKind::Frontier,
        "v7 generation-zero slot is not a FRONTIER"
    );
    let generation_zero_frontier = WalV7Frontier::decode_body(&generation_zero.body)?;
    generation_zero_frontier.validate_generation_zero(&header_digest, HEADER_LEN_U64)?;
    let generation_zero_digest = frame_record_digest(&generation_zero_bytes)?;
    let canonical_generation_zero = encode_generation_zero_frontier(header_digest)?;
    ensure!(
        generation_zero_bytes == canonical_generation_zero,
        "v7 generation-zero FRONTIER is not canonical"
    );

    let mut logical_hasher = Sha256::new();
    logical_hasher.update(header_bytes);
    let mut checkpoints = Vec::new();
    checkpoints
        .try_reserve(
            candidates
                .len()
                .min(usize::try_from(file_len / FRAME_LEN_U64)?),
        )
        .context("reserve v7 prefix checkpoints")?;
    checkpoints
        .try_reserve(1)
        .context("reserve initial v7 prefix checkpoint")?;
    checkpoints.push(WalV7PrefixCheckpoint {
        data_start: HEADER_LEN_U64,
        data_end: HEADER_LEN_U64,
        last_commit_ts: 0,
        prefix_digest: header_digest,
    });
    let mut frontiers = Vec::new();
    frontiers
        .try_reserve(
            candidates
                .len()
                .min(usize::try_from(file_len / FRAME_LEN_U64)?),
        )
        .context("reserve v7 prefix FRONTIER metadata")?;
    frontiers
        .try_reserve(1)
        .context("reserve generation-zero FRONTIER metadata")?;
    frontiers.push(WalV7PrefixFrontier {
        frame_offset: HEADER_LEN_U64,
        frontier: generation_zero_frontier,
        frame_digest: generation_zero_digest,
        chain_breaks_through: 0,
    });

    let mut maximum_candidate_end = HEADER_LEN_U64;
    for candidate in candidates {
        if !candidate_has_valid_frame_bounds(candidate, file_len) {
            verification.reject(
                candidate.frame_offset,
                "candidate marker or covered-prefix bounds are invalid",
            )?;
            continue;
        }
        maximum_candidate_end = maximum_candidate_end.max(candidate.frontier.durable_end);
    }
    for anchor in active_anchors {
        maximum_candidate_end = maximum_candidate_end.max(anchor.durable_end);
    }

    let mut cursor = HEADER_LEN_U64 + FRAME_LEN_U64;
    let mut last_commit_ts = 0_u64;
    let mut last_recorded_at = None;
    let mut stopped_at = None;

    while cursor < maximum_candidate_end {
        let frame_offset = cursor;
        let encoded_frame = read_frame(reader, frame_offset)?;
        let decoded = match decode_frame_structural(&encoded_frame, frame_offset, &header_digest) {
            Ok(frame) => frame,
            Err(error) => {
                stopped_at = Some(rejected_prefix(frame_offset, error));
                break;
            }
        };

        match decoded.kind {
            WalV7FrameKind::Data => {
                let (first_fragment, first_payload) =
                    match WalV7DataFragmentHeader::decode_body(&decoded.body) {
                        Ok(fragment) => fragment,
                        Err(error) => {
                            stopped_at = Some(rejected_prefix(frame_offset, error));
                            break;
                        }
                    };
                let expected_ticket = u64::try_from(checkpoints.len() - 1)?;
                if first_fragment.segment_ticket != expected_ticket
                    || first_fragment.fragment_index != 0
                {
                    stopped_at = Some(rejected_prefix(
                        frame_offset,
                        anyhow::anyhow!("v7 DATA ticket order or first fragment index is invalid"),
                    ));
                    break;
                }

                let batch_len = match usize::try_from(first_fragment.batch_bytes) {
                    Ok(length) => length,
                    Err(error) => {
                        stopped_at = Some(rejected_prefix(frame_offset, error));
                        break;
                    }
                };
                let max_batch_len = WAL_V7_LOGICAL_BATCH_HEADER_LEN
                    .checked_add(LIVE_WAL_V5_LIMITS.max_batch_data_bytes)
                    .ok_or_else(|| anyhow::anyhow!("v7 logical batch limit overflows"))?;
                if batch_len > max_batch_len {
                    stopped_at = Some(rejected_prefix(
                        frame_offset,
                        anyhow::anyhow!("v7 DATA batch exceeds configured byte limit"),
                    ));
                    break;
                }

                let fragment_count = u64::from(first_fragment.fragment_count);
                let batch_end = match fragment_count
                    .checked_mul(FRAME_LEN_U64)
                    .and_then(|span| frame_offset.checked_add(span))
                {
                    Some(end) if end <= maximum_candidate_end => end,
                    _ => {
                        stopped_at = Some(rejected_prefix(
                            frame_offset,
                            anyhow::anyhow!("v7 DATA batch extends beyond the verified prefix"),
                        ));
                        break;
                    }
                };

                let mut batch_bytes = Vec::new();
                batch_bytes
                    .try_reserve_exact(batch_len)
                    .context("reserve v7 logical batch buffer")?;
                batch_bytes.extend_from_slice(first_payload);
                let fragment_count = usize::try_from(first_fragment.fragment_count)?;
                let mut fragment_error = None;
                for fragment_index in 1..fragment_count {
                    let fragment_offset = frame_offset
                        .checked_add(
                            u64::try_from(fragment_index)?
                                .checked_mul(FRAME_LEN_U64)
                                .ok_or_else(|| anyhow::anyhow!("v7 fragment offset overflows"))?,
                        )
                        .ok_or_else(|| anyhow::anyhow!("v7 fragment offset overflows"))?;
                    let encoded_fragment = read_frame(reader, fragment_offset)?;
                    let fragment_frame = match decode_frame_structural(
                        &encoded_fragment,
                        fragment_offset,
                        &header_digest,
                    ) {
                        Ok(frame) => frame,
                        Err(error) => {
                            fragment_error = Some(error);
                            break;
                        }
                    };
                    if fragment_frame.kind != WalV7FrameKind::Data {
                        fragment_error = Some(anyhow::anyhow!("v7 DATA batch is interleaved"));
                        break;
                    }
                    let (fragment, payload) =
                        match WalV7DataFragmentHeader::decode_body(&fragment_frame.body) {
                            Ok(fragment) => fragment,
                            Err(error) => {
                                fragment_error = Some(error);
                                break;
                            }
                        };
                    if fragment.segment_ticket != first_fragment.segment_ticket
                        || fragment.fragment_index != u32::try_from(fragment_index)?
                        || fragment.fragment_count != first_fragment.fragment_count
                        || fragment.batch_bytes != first_fragment.batch_bytes
                    {
                        fragment_error = Some(anyhow::anyhow!("v7 DATA fragments disagree"));
                        break;
                    }
                    batch_bytes.extend_from_slice(payload);
                }
                if let Some(error) = fragment_error {
                    stopped_at = Some(rejected_prefix(frame_offset, error));
                    break;
                }
                if batch_bytes.len() != batch_len
                    || batch_bytes.len() < WAL_V7_LOGICAL_BATCH_HEADER_LEN
                {
                    stopped_at = Some(rejected_prefix(
                        frame_offset,
                        anyhow::anyhow!("v7 DATA fragments have an invalid total length"),
                    ));
                    break;
                }

                let data = &batch_bytes[WAL_V7_LOGICAL_BATCH_HEADER_LEN..];
                let logical_header = match WalV7LogicalBatchHeader::decode(
                    &batch_bytes[..WAL_V7_LOGICAL_BATCH_HEADER_LEN],
                    data,
                    first_fragment.segment_ticket,
                    first_fragment.batch_bytes,
                    LIVE_WAL_V5_LIMITS,
                ) {
                    Ok(header) => header,
                    Err(error) => {
                        stopped_at = Some(rejected_prefix(frame_offset, error));
                        break;
                    }
                };
                let recorded_at = RecordedAt {
                    secs: logical_header.recorded_at_secs,
                    nanos: logical_header.recorded_at_nanos,
                };
                let batch = match decode_wal_entry_stream(
                    logical_header.commit_ts,
                    recorded_at,
                    logical_header.entry_count,
                    data,
                    LIVE_WAL_V5_LIMITS,
                ) {
                    Ok(batch) => batch,
                    Err(error) => {
                        stopped_at = Some(rejected_prefix(frame_offset, error));
                        break;
                    }
                };
                if batch.commit_ts <= last_commit_ts
                    || last_recorded_at.is_some_and(|previous| batch.recorded_at < previous)
                {
                    stopped_at = Some(rejected_prefix(
                        frame_offset,
                        anyhow::anyhow!("v7 commit timestamps or recorded times are out of order"),
                    ));
                    break;
                }

                logical_hasher.update(&batch_bytes);
                checkpoints
                    .try_reserve(1)
                    .context("grow v7 prefix checkpoint metadata")?;
                checkpoints.push(WalV7PrefixCheckpoint {
                    data_start: frame_offset,
                    data_end: batch_end,
                    last_commit_ts: batch.commit_ts,
                    prefix_digest: logical_hasher.clone().finalize().into(),
                });
                last_commit_ts = batch.commit_ts;
                last_recorded_at = Some(batch.recorded_at);
                cursor = batch_end;
            }
            WalV7FrameKind::Frontier => {
                let frontier = match WalV7Frontier::decode_body(&decoded.body) {
                    Ok(frontier) => frontier,
                    Err(error) => {
                        stopped_at = Some(rejected_prefix(frame_offset, error));
                        break;
                    }
                };
                let frame_digest = frame_record_digest(&encoded_frame)?;
                let predecessor = match prefix_predecessor(&frontiers, frontier.durable_end) {
                    Ok(predecessor) => predecessor,
                    Err(error) => {
                        stopped_at = Some(rejected_prefix(frame_offset, error));
                        break;
                    }
                };
                if let Err(error) =
                    validate_named_frontier_prefix(frontier, predecessor, &checkpoints)
                {
                    stopped_at = Some(rejected_prefix(frame_offset, error));
                    break;
                }
                if let Err(error) = decode_frontier_successor(
                    &encoded_frame,
                    frame_offset,
                    &header_digest,
                    predecessor.frontier,
                    predecessor.frame_offset,
                    &predecessor.frame_digest,
                ) {
                    stopped_at = Some(rejected_prefix(frame_offset, error));
                    break;
                }

                let previous_physical = frontiers
                    .last()
                    .context("v7 frontier prefix lost generation zero")?;
                let follows_physical_chain = frontier.previous_frontier_offset
                    == previous_physical.frame_offset
                    && frontier.previous_frontier_digest == previous_physical.frame_digest
                    && previous_physical.frontier.generation.checked_add(1)
                        == Some(frontier.generation);
                let chain_breaks_through = previous_physical
                    .chain_breaks_through
                    .checked_add(u64::from(!follows_physical_chain))
                    .ok_or_else(|| anyhow::anyhow!("v7 FRONTIER chain-break count overflows"))?;
                frontiers
                    .try_reserve(1)
                    .context("grow v7 prefix FRONTIER metadata")?;
                frontiers.push(WalV7PrefixFrontier {
                    frame_offset,
                    frontier,
                    frame_digest,
                    chain_breaks_through,
                });
                cursor = cursor
                    .checked_add(FRAME_LEN_U64)
                    .ok_or_else(|| anyhow::anyhow!("v7 prefix offset overflows"))?;
            }
        }
    }

    verification.verified_prefix_end = cursor;
    verification.stopped_at = stopped_at.clone();
    for candidate in candidates {
        if !candidate_has_valid_frame_bounds(candidate, file_len) {
            continue;
        }
        if candidate.frontier.durable_end > cursor {
            let reason = stopped_at.as_ref().map_or_else(
                || "candidate prefix was not scanned".to_owned(),
                |stop| {
                    format!(
                        "candidate prefix extends past invalid frame at {}: {}",
                        stop.frame_offset, stop.reason
                    )
                },
            );
            verification.reject(candidate.frame_offset, reason)?;
            continue;
        }
        match verify_candidate_prefix(
            candidate,
            &header_digest,
            generation_zero_frontier,
            generation_zero_digest,
            &frontiers,
            &checkpoints,
        ) {
            Ok(()) => {
                verification
                    .verified_candidates
                    .try_reserve(1)
                    .context("grow verified v7 candidate metadata")?;
                verification.verified_candidates.push(candidate.clone());
            }
            Err(error) => verification.reject(candidate.frame_offset, error)?,
        }
    }

    for anchor in active_anchors {
        verify_active_anchor_prefix(anchor, &frontiers, &checkpoints, cursor)?;
    }

    Ok(verification)
}

/// Select an authoritative v7 recovery boundary from discovered and verified
/// candidates. The caller must hold exclusive recovery ownership, and discovery
/// must describe this unchanged image and `file_len` snapshot. Active recovery
/// may fall back above every verified anchor; Sealing and Sealed recovery
/// require the exact intent-bound terminal image.
pub(crate) fn select_recovery_candidate<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    discovery: &WalV7FrontierDiscovery,
    authority: WalV7RecoveryAuthority,
) -> Result<WalV7RecoverySelection> {
    match authority {
        WalV7RecoveryAuthority::Active {
            predecessor,
            durable_anchors,
        } => {
            let anchor_requirements = WalV7ActiveAnchorRequirements::new(&durable_anchors)?;
            let verification = verify_frontier_candidate_prefixes_with_anchors(
                reader,
                file_len,
                &discovery.candidates,
                &durable_anchors,
                Some(predecessor),
            )?;
            let selected = verification
                .verified_candidates
                .iter()
                .filter(|candidate| anchor_requirements.covers(candidate))
                .max_by_key(|candidate| candidate.frame_offset)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no fully verified Active v7 FRONTIER satisfies all durable anchors"
                    )
                })?;
            let identity = durable_anchors[0];
            let active_boundary = ActiveBoundary {
                timeline_id: identity.timeline_id,
                archive_epoch_id: identity.archive_epoch_id,
                segment_id: identity.segment_id,
                incarnation: identity.incarnation,
                ticket_end: selected.frontier.ticket_end,
                durable_end: selected.frontier.durable_end,
                last_commit_ts: selected.frontier.last_commit_ts,
                prefix_digest: selected.frontier.prefix_digest,
            };
            active_boundary.validate()?;
            let diagnostics = recovery_diagnostics(
                discovery,
                &verification,
                &selected,
                file_len,
                Some(&active_boundary),
            )?;

            Ok(WalV7RecoverySelection {
                candidate: selected,
                active_boundary,
                diagnostics,
            })
        }
        WalV7RecoveryAuthority::Sealing {
            predecessor,
            boundary,
        }
        | WalV7RecoveryAuthority::Sealed {
            predecessor,
            boundary,
        } => {
            boundary.validate()?;
            ensure!(
                file_len == boundary.sealed_end,
                "immutable v7 WAL length differs from its durable sealed boundary"
            );
            let verification = verify_frontier_candidate_prefixes_with_anchors(
                reader,
                file_len,
                &discovery.candidates,
                std::slice::from_ref(&boundary.active),
                Some(predecessor),
            )?;
            let terminal_offset = boundary
                .sealed_end
                .checked_sub(FRAME_LEN_U64)
                .ok_or_else(|| anyhow::anyhow!("immutable v7 terminal offset underflows"))?;
            let selected = verification
                .verified_candidates
                .iter()
                .find(|candidate| candidate.frame_offset == terminal_offset)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "immutable v7 terminal FRONTIER is absent or its prefix is invalid"
                    )
                })?;
            ensure!(
                selected.frame_digest == boundary.terminal_frontier_digest,
                "immutable v7 terminal FRONTIER digest differs from its durable boundary"
            );
            ensure!(
                selected.frontier.ticket_end == boundary.active.ticket_end
                    && selected.frontier.durable_end == boundary.active.durable_end
                    && selected.frontier.last_commit_ts == boundary.active.last_commit_ts
                    && selected.frontier.prefix_digest == boundary.active.prefix_digest,
                "immutable v7 terminal FRONTIER differs from its logical durable anchor"
            );
            ensure!(
                selected.frontier.durable_end == terminal_offset,
                "immutable v7 terminal FRONTIER does not cover the complete physical image"
            );
            verify_immutable_wal_digest(reader, file_len, boundary.wal_digest)?;
            let diagnostics =
                recovery_diagnostics(discovery, &verification, &selected, file_len, None)?;

            Ok(WalV7RecoverySelection {
                candidate: selected,
                active_boundary: boundary.active,
                diagnostics,
            })
        }
    }
}

fn validate_anchor_identity(anchor: &ActiveBoundary, header: WalV7Header) -> Result<()> {
    anchor.validate()?;
    ensure!(
        anchor.timeline_id == header.timeline_id.0
            && anchor.archive_epoch_id == header.archive_epoch_id.0
            && anchor.segment_id == header.segment_id.0
            && anchor.incarnation == header.incarnation,
        "Active v7 durable anchor identifies a different WAL segment"
    );
    Ok(())
}

fn verify_active_anchor_prefix(
    anchor: &ActiveBoundary,
    frontiers: &[WalV7PrefixFrontier],
    checkpoints: &[WalV7PrefixCheckpoint],
    verified_prefix_end: u64,
) -> Result<()> {
    ensure!(
        anchor.durable_end <= verified_prefix_end,
        "Active v7 durable anchor extends beyond the verified physical prefix"
    );
    let ticket_end = usize::try_from(anchor.ticket_end)?;
    let checkpoint = checkpoints
        .get(ticket_end)
        .context("Active v7 durable anchor ticket end exceeds verified DATA")?;

    if ticket_end == 0 {
        ensure!(
            anchor.durable_end == HEADER_LEN_U64
                && anchor.last_commit_ts == 0
                && anchor.prefix_digest == checkpoint.prefix_digest,
            "empty Active v7 durable anchor differs from the immutable header"
        );
        return Ok(());
    }

    ensure!(
        checkpoint.last_commit_ts == anchor.last_commit_ts
            && checkpoint.prefix_digest == anchor.prefix_digest,
        "Active v7 durable anchor timestamp or logical prefix digest mismatch"
    );
    let complete_batch_count =
        checkpoints[1..].partition_point(|batch| batch.data_end <= anchor.durable_end);
    ensure!(
        u64::try_from(complete_batch_count)? == anchor.ticket_end,
        "Active v7 durable anchor covers a different DATA ticket count"
    );
    if let Some(partial_batch) = checkpoints[1..].get(complete_batch_count) {
        ensure!(
            partial_batch.data_start >= anchor.durable_end,
            "Active v7 durable anchor splits a DATA batch"
        );
    }

    let predecessor = prefix_predecessor(frontiers, anchor.durable_end)?;
    let predecessor_end = predecessor
        .frame_offset
        .checked_add(FRAME_LEN_U64)
        .ok_or_else(|| anyhow::anyhow!("Active v7 predecessor end overflows"))?;
    let expected_durable_end = checkpoint.data_end.max(predecessor_end);
    ensure!(
        anchor.durable_end == expected_durable_end,
        "Active v7 durable anchor does not match its exact physical prefix"
    );
    Ok(())
}

fn candidate_covers_anchor(candidate: &WalV7FrontierCandidate, anchor: &ActiveBoundary) -> bool {
    if candidate.frontier.ticket_end < anchor.ticket_end
        || candidate.frontier.durable_end < anchor.durable_end
        || candidate.frontier.last_commit_ts < anchor.last_commit_ts
    {
        return false;
    }
    if candidate.frontier.ticket_end == anchor.ticket_end {
        candidate.frontier.durable_end == anchor.durable_end
            && candidate.frontier.last_commit_ts == anchor.last_commit_ts
            && candidate.frontier.prefix_digest == anchor.prefix_digest
    } else {
        candidate.frontier.durable_end > anchor.durable_end
            && candidate.frontier.last_commit_ts > anchor.last_commit_ts
    }
}

fn verify_immutable_wal_digest<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    expected_digest: [u8; 32],
) -> Result<()> {
    reader
        .seek(SeekFrom::Start(0))
        .context("seek immutable v7 WAL image for digest")?;
    let mut remaining = file_len;
    let mut buffer = [0_u8; IMMUTABLE_HASH_BUFFER_LEN];
    let mut hasher = Sha256::new();
    while remaining > 0 {
        let bytes_to_read = usize::try_from(remaining.min(IMMUTABLE_HASH_BUFFER_LEN as u64))?;
        reader
            .read_exact(&mut buffer[..bytes_to_read])
            .context("read immutable v7 WAL image for digest")?;
        hasher.update(&buffer[..bytes_to_read]);
        remaining -= u64::try_from(bytes_to_read)?;
    }
    let actual_digest: [u8; 32] = hasher.finalize().into();
    ensure!(
        actual_digest == expected_digest,
        "immutable v7 WAL image digest mismatch"
    );
    Ok(())
}

fn recovery_diagnostics(
    discovery: &WalV7FrontierDiscovery,
    verification: &WalV7FrontierPrefixVerification,
    selected: &WalV7FrontierCandidate,
    file_len: u64,
    active_boundary: Option<&ActiveBoundary>,
) -> Result<WalV7RecoveryDiagnostics> {
    let rejected_frontier_count = discovery
        .rejected_frontier_count
        .checked_add(verification.rejected_candidate_count)
        .ok_or_else(|| anyhow::anyhow!("v7 recovery rejected-frontier count overflows"))?;
    let mut rejected_frontiers = Vec::new();
    rejected_frontiers
        .try_reserve(
            discovery
                .rejected_frontiers
                .len()
                .saturating_add(verification.rejected_candidates.len())
                .min(MAX_REJECTED_FRONTIER_DIAGNOSTICS),
        )
        .context("reserve v7 recovery diagnostics")?;
    for rejected in discovery
        .rejected_frontiers
        .iter()
        .chain(&verification.rejected_candidates)
    {
        rejected_frontiers.push(rejected.clone());
    }
    rejected_frontiers.sort_unstable_by_key(|rejected| std::cmp::Reverse(rejected.frame_offset));
    rejected_frontiers.truncate(MAX_REJECTED_FRONTIER_DIAGNOSTICS);

    let has_newer_candidate = discovery
        .candidates
        .iter()
        .any(|candidate| candidate.frame_offset > selected.frame_offset)
        || discovery
            .rejected_frontiers
            .first()
            .is_some_and(|rejected| rejected.frame_offset > selected.frame_offset)
        || verification
            .rejected_candidates
            .first()
            .is_some_and(|rejected| rejected.frame_offset > selected.frame_offset);
    let fallback_reason = has_newer_candidate.then(|| {
        bounded_reason(format!(
            "selected frontier at {} after rejecting or excluding newer physical candidates",
            selected.frame_offset
        ))
    });

    let discarded_physical_tail = if let Some(boundary) = active_boundary {
        let start = if boundary.ticket_end == 0 {
            HEADER_LEN_U64 + FRAME_LEN_U64
        } else {
            boundary.durable_end
        };
        ensure!(
            start <= file_len,
            "Active v7 selected boundary exceeds physical WAL length"
        );
        (start < file_len).then_some((start, file_len))
    } else {
        None
    };

    Ok(WalV7RecoveryDiagnostics {
        selected_frame_offset: selected.frame_offset,
        rejected_frontiers,
        rejected_frontier_count,
        discarded_physical_tail,
        fallback_reason,
    })
}

fn candidate_has_valid_frame_bounds(candidate: &WalV7FrontierCandidate, file_len: u64) -> bool {
    candidate.frame_offset >= HEADER_LEN_U64
        && candidate.frame_offset.is_multiple_of(FRAME_LEN_U64)
        && candidate
            .frame_offset
            .checked_add(FRAME_LEN_U64)
            .is_some_and(|end| end <= file_len)
        && candidate.frontier.durable_end >= HEADER_LEN_U64
        && candidate.frontier.durable_end <= candidate.frame_offset
        && candidate.frontier.durable_end.is_multiple_of(FRAME_LEN_U64)
}

fn verify_candidate_prefix(
    candidate: &WalV7FrontierCandidate,
    header_digest: &[u8; 32],
    generation_zero: WalV7Frontier,
    generation_zero_digest: [u8; 32],
    frontiers: &[WalV7PrefixFrontier],
    checkpoints: &[WalV7PrefixCheckpoint],
) -> Result<()> {
    if candidate.frontier.generation == 0 {
        ensure!(
            candidate.frame_offset == HEADER_LEN_U64
                && candidate.frontier == generation_zero
                && candidate.frame_digest == generation_zero_digest,
            "v7 generation-zero candidate does not match its canonical frame"
        );
        return Ok(());
    }
    ensure!(
        candidate.frame_offset != HEADER_LEN_U64,
        "v7 generation-zero FRONTIER is not at offset 4096"
    );

    let predecessor = prefix_predecessor(frontiers, candidate.frontier.durable_end)?;
    validate_named_frontier_prefix(candidate.frontier, predecessor, checkpoints)?;
    let encoded = encode_frontier_successor(
        candidate.frontier,
        predecessor.frontier,
        predecessor.frame_offset,
        &predecessor.frame_digest,
        candidate.frame_offset,
        *header_digest,
    )?;
    ensure!(
        frame_record_digest(&encoded)? == candidate.frame_digest,
        "v7 candidate marker digest does not match its canonical frontier"
    );
    Ok(())
}

fn prefix_predecessor(
    frontiers: &[WalV7PrefixFrontier],
    durable_end: u64,
) -> Result<&WalV7PrefixFrontier> {
    let count = frontiers.partition_point(|frontier| {
        frontier
            .frame_offset
            .checked_add(FRAME_LEN_U64)
            .is_some_and(|end| end <= durable_end)
    });
    ensure!(count > 0, "v7 FRONTIER prefix omits generation zero");
    let predecessor = &frontiers[count - 1];
    ensure!(
        predecessor.chain_breaks_through == 0,
        "v7 FRONTIER prefix contains a fork or orphan control frame"
    );
    Ok(predecessor)
}

fn validate_named_frontier_prefix(
    frontier: WalV7Frontier,
    predecessor: &WalV7PrefixFrontier,
    checkpoints: &[WalV7PrefixCheckpoint],
) -> Result<()> {
    let ticket_end = usize::try_from(frontier.ticket_end)?;
    ensure!(
        ticket_end < checkpoints.len(),
        "v7 FRONTIER ticket end exceeds verified DATA"
    );
    let checkpoint = checkpoints[ticket_end];
    ensure!(
        checkpoint.last_commit_ts == frontier.last_commit_ts
            && checkpoint.prefix_digest == frontier.prefix_digest,
        "v7 FRONTIER logical timestamp or prefix digest mismatch"
    );

    let covered_batches = &checkpoints[1..];
    let complete_batch_count =
        covered_batches.partition_point(|batch| batch.data_end <= frontier.durable_end);
    if let Some(partial_batch) = covered_batches.get(complete_batch_count) {
        ensure!(
            partial_batch.data_start >= frontier.durable_end,
            "v7 FRONTIER durable_end splits a DATA batch"
        );
    }
    ensure!(
        u64::try_from(complete_batch_count)? == frontier.ticket_end,
        "v7 FRONTIER prefix contains a different DATA ticket count"
    );

    let data_end = if ticket_end == 0 {
        HEADER_LEN_U64
    } else {
        checkpoint.data_end
    };
    let predecessor_end = predecessor
        .frame_offset
        .checked_add(FRAME_LEN_U64)
        .ok_or_else(|| anyhow::anyhow!("v7 predecessor frame end overflows"))?;
    let expected_durable_end = data_end.max(predecessor_end);
    ensure!(
        frontier.durable_end == expected_durable_end,
        "v7 FRONTIER durable_end does not match its DATA and control prefix"
    );
    Ok(())
}

fn read_frame<R: Read>(reader: &mut R, frame_offset: u64) -> Result<[u8; WAL_V7_FRAME_LEN]> {
    let mut frame = [0_u8; WAL_V7_FRAME_LEN];
    reader
        .read_exact(&mut frame)
        .with_context(|| format!("read v7 WAL frame at {frame_offset}"))?;
    Ok(frame)
}

fn rejected_prefix(frame_offset: u64, error: impl std::fmt::Display) -> WalV7RejectedFrontier {
    WalV7RejectedFrontier {
        frame_offset,
        reason: bounded_reason(error),
    }
}

fn frontier_candidate(
    frame: DecodedWalV7Frame,
    encoded_frame: &[u8],
) -> Result<Option<WalV7FrontierCandidate>> {
    if frame.kind != WalV7FrameKind::Frontier {
        return Ok(None);
    }
    let frontier = WalV7Frontier::decode_body(&frame.body)?;
    Ok(Some(WalV7FrontierCandidate {
        frame_offset: frame.frame_offset,
        frontier,
        frame_digest: frame_record_digest(encoded_frame)?,
    }))
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read, Seek, SeekFrom};

    use anyhow::Result;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::pitr::{
        ArchiveEpochId, ChainAnchor, LIVE_WAL_V5_LIMITS, RecordedAt, SegmentId, TimelineId,
        WalBatch, WalEntry, encode_v5_batch,
    };
    use crate::wal::v7::codec::{
        WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY, WalV7Frontier, WalV7LogicalBatchHeader,
        decode_frame_structural, encode_data_frame, encode_frontier_successor,
        encode_generation_zero_frontier,
    };

    const HEADER_DIGEST: [u8; 32] = [0x5a; 32];

    fn install_frame(image: &mut Vec<u8>, frame_offset: u64, frame: &[u8]) -> Result<()> {
        let start = usize::try_from(frame_offset)?;
        let end = start
            .checked_add(frame.len())
            .ok_or_else(|| anyhow::anyhow!("test frame end overflows"))?;
        if image.len() < end {
            image.resize(end, 0);
        }
        image[start..end].copy_from_slice(frame);

        Ok(())
    }

    fn generation_zero() -> Result<(WalV7Frontier, [u8; 32])> {
        let frame = encode_generation_zero_frontier(HEADER_DIGEST)?;
        let decoded = decode_frame_structural(&frame, HEADER_LEN_U64, &HEADER_DIGEST)?;
        let frontier = WalV7Frontier::decode_body(&decoded.body)?;
        Ok((frontier, frame_record_digest(&frame)?))
    }

    fn successor(
        previous: WalV7Frontier,
        previous_offset: u64,
        previous_digest: [u8; 32],
        frame_offset: u64,
        ticket_end: u64,
        durable_end: u64,
        last_commit_ts: u64,
    ) -> Result<([u8; WAL_V7_FRAME_LEN], WalV7Frontier)> {
        let frontier = WalV7Frontier {
            generation: previous.generation + 1,
            ticket_end,
            durable_end,
            last_commit_ts,
            prefix_digest: [last_commit_ts as u8; 32],
            previous_frontier_offset: previous_offset,
            previous_frontier_digest: previous_digest,
        };
        let frame = encode_frontier_successor(
            frontier,
            previous,
            previous_offset,
            &previous_digest,
            frame_offset,
            HEADER_DIGEST,
        )?;
        Ok((frame, frontier))
    }

    fn test_header() -> WalV7Header {
        let archive_epoch_id = ArchiveEpochId([0x22; 16]);
        WalV7Header {
            timeline_id: TimelineId([0x11; 16]),
            archive_epoch_id,
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Genesis { archive_epoch_id },
            incarnation: [0x44; 16],
        }
    }

    struct TestWalImage {
        image: Vec<u8>,
        header_digest: [u8; 32],
        frontier_zero: WalV7Frontier,
        frontier_zero_digest: [u8; 32],
    }

    struct SuccessorSpec {
        previous: WalV7Frontier,
        previous_offset: u64,
        previous_digest: [u8; 32],
        frame_offset: u64,
        ticket_end: u64,
        durable_end: u64,
        last_commit_ts: u64,
        prefix_digest: [u8; 32],
    }

    fn test_image() -> Result<TestWalImage> {
        let header_bytes = test_header().encode()?;
        let header_digest: [u8; 32] = Sha256::digest(header_bytes).into();
        let generation_zero_frame = encode_generation_zero_frontier(header_digest)?;
        let generation_zero = WalV7Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let generation_zero_digest = frame_record_digest(&generation_zero_frame)?;
        let mut image = header_bytes.to_vec();
        install_frame(&mut image, HEADER_LEN_U64, &generation_zero_frame)?;
        Ok(TestWalImage {
            image,
            header_digest,
            frontier_zero: generation_zero,
            frontier_zero_digest: generation_zero_digest,
        })
    }

    fn append_data_batch(
        image: &mut Vec<u8>,
        frame_offset: u64,
        header_digest: [u8; 32],
        ticket: u64,
        commit_ts: u64,
        recorded_at_secs: i64,
        value: &[u8],
    ) -> Result<(u64, Vec<u8>)> {
        let batch = WalBatch {
            commit_ts,
            recorded_at: RecordedAt {
                secs: recorded_at_secs,
                nanos: 123,
            },
            entries: vec![WalEntry::Put {
                key: format!("key-{ticket}").into_bytes(),
                value: value.to_vec(),
            }],
        };
        let encoded_v5 = encode_v5_batch(&batch, LIVE_WAL_V5_LIMITS)?;
        let data_len = usize::try_from(u32::from_be_bytes(
            encoded_v5[24..28]
                .try_into()
                .expect("v5 data length is four bytes"),
        ))?;
        let data_end = crate::pitr::WAL_V5_BATCH_HEADER_LEN
            .checked_add(data_len)
            .ok_or_else(|| anyhow::anyhow!("test entry stream end overflows"))?;
        let data = &encoded_v5[crate::pitr::WAL_V5_BATCH_HEADER_LEN..data_end];
        let logical_header = WalV7LogicalBatchHeader {
            segment_ticket: ticket,
            commit_ts,
            recorded_at_secs,
            recorded_at_nanos: batch.recorded_at.nanos,
            entry_count: u32::try_from(batch.entries.len())?,
        }
        .encode(data, LIVE_WAL_V5_LIMITS)?;
        let mut logical_batch = Vec::with_capacity(logical_header.len() + data.len());
        logical_batch.extend_from_slice(&logical_header);
        logical_batch.extend_from_slice(data);

        let fragment_count = logical_batch
            .len()
            .div_ceil(WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY);
        let fragment_count_u32 = u32::try_from(fragment_count)?;
        for (fragment_index, payload) in logical_batch
            .chunks(WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY)
            .enumerate()
        {
            let fragment_offset = frame_offset
                .checked_add(
                    u64::try_from(fragment_index)?
                        .checked_mul(FRAME_LEN_U64)
                        .ok_or_else(|| anyhow::anyhow!("test fragment offset overflows"))?,
                )
                .ok_or_else(|| anyhow::anyhow!("test fragment offset overflows"))?;
            let body = WalV7DataFragmentHeader {
                segment_ticket: ticket,
                fragment_index: u32::try_from(fragment_index)?,
                fragment_count: fragment_count_u32,
                batch_bytes: u64::try_from(logical_batch.len())?,
            }
            .encode_body(payload)?;
            let frame = encode_data_frame(header_digest, fragment_offset, &body)?;
            install_frame(image, fragment_offset, &frame)?;
        }

        let frame_span = u64::try_from(fragment_count)?
            .checked_mul(FRAME_LEN_U64)
            .ok_or_else(|| anyhow::anyhow!("test batch frame span overflows"))?;
        let batch_end = frame_offset
            .checked_add(frame_span)
            .ok_or_else(|| anyhow::anyhow!("test batch end overflows"))?;
        Ok((batch_end, logical_batch))
    }

    fn append_successor(
        image: &mut Vec<u8>,
        header_digest: [u8; 32],
        spec: SuccessorSpec,
    ) -> Result<(WalV7Frontier, [u8; 32])> {
        let frontier = WalV7Frontier {
            generation: spec.previous.generation + 1,
            ticket_end: spec.ticket_end,
            durable_end: spec.durable_end,
            last_commit_ts: spec.last_commit_ts,
            prefix_digest: spec.prefix_digest,
            previous_frontier_offset: spec.previous_offset,
            previous_frontier_digest: spec.previous_digest,
        };
        let frame = encode_frontier_successor(
            frontier,
            spec.previous,
            spec.previous_offset,
            &spec.previous_digest,
            spec.frame_offset,
            header_digest,
        )?;
        let digest = frame_record_digest(&frame)?;
        install_frame(image, spec.frame_offset, &frame)?;
        Ok((frontier, digest))
    }

    fn logical_prefix_digest(header: &[u8], batches: &[&[u8]]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(header);
        for batch in batches {
            hasher.update(batch);
        }
        hasher.finalize().into()
    }

    fn discover_and_verify(image: &[u8]) -> Result<WalV7FrontierPrefixVerification> {
        let header_digest = test_header().digest()?;
        let mut reader = Cursor::new(image);
        let discovery =
            discover_frontier_candidates(&mut reader, u64::try_from(image.len())?, &header_digest)?;
        verify_frontier_candidate_prefixes(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery.candidates,
        )
    }

    fn boundary_for(frontier: WalV7Frontier, header: WalV7Header) -> ActiveBoundary {
        ActiveBoundary {
            timeline_id: header.timeline_id.0,
            archive_epoch_id: header.archive_epoch_id.0,
            segment_id: header.segment_id.0,
            incarnation: header.incarnation,
            ticket_end: frontier.ticket_end,
            durable_end: frontier.durable_end,
            last_commit_ts: frontier.last_commit_ts,
            prefix_digest: frontier.prefix_digest,
        }
    }

    fn two_batch_image() -> Result<(Vec<u8>, ActiveBoundary, ActiveBoundary)> {
        let TestWalImage {
            mut image,
            header_digest,
            frontier_zero,
            frontier_zero_digest: digest_zero,
        } = test_image()?;
        let header_bytes = image[..WAL_V7_HEADER_LEN].to_vec();
        let (first_data_end, first_batch) =
            append_data_batch(&mut image, 8192, header_digest, 0, 10, 100, b"first")?;
        let first_prefix_digest = logical_prefix_digest(&header_bytes, &[&first_batch]);
        let (frontier_one, digest_one) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: digest_zero,
                frame_offset: first_data_end,
                ticket_end: 1,
                durable_end: first_data_end,
                last_commit_ts: 10,
                prefix_digest: first_prefix_digest,
            },
        )?;
        let second_data_start = first_data_end + FRAME_LEN_U64;
        let (second_data_end, second_batch) = append_data_batch(
            &mut image,
            second_data_start,
            header_digest,
            1,
            20,
            101,
            b"second",
        )?;
        let second_prefix_digest =
            logical_prefix_digest(&header_bytes, &[&first_batch, &second_batch]);
        let (frontier_two, _) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_one,
                previous_offset: first_data_end,
                previous_digest: digest_one,
                frame_offset: second_data_end,
                ticket_end: 2,
                durable_end: second_data_end,
                last_commit_ts: 20,
                prefix_digest: second_prefix_digest,
            },
        )?;

        let header = test_header();
        Ok((
            image,
            boundary_for(frontier_one, header),
            boundary_for(frontier_two, header),
        ))
    }

    fn discover_image(image: &[u8]) -> Result<WalV7FrontierDiscovery> {
        let mut reader = Cursor::new(image);
        discover_frontier_candidates(
            &mut reader,
            u64::try_from(image.len())?,
            &test_header().digest()?,
        )
    }

    #[test]
    fn selects_newest_physical_candidate_containing_all_active_anchors() -> Result<()> {
        let (image, first_anchor, newest_anchor) = two_batch_image()?;
        let discovery = discover_image(&image)?;
        let mut reader = Cursor::new(image.as_slice());

        let selection = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: test_header().predecessor,
                durable_anchors: vec![first_anchor, newest_anchor],
            },
        )?;

        assert_eq!(selection.active_boundary.ticket_end, 2);
        assert_eq!(
            selection.candidate.frame_offset,
            u64::try_from(image.len())? - FRAME_LEN_U64
        );
        assert_eq!(
            selection.diagnostics.selected_frame_offset,
            selection.candidate.frame_offset
        );
        Ok(())
    }

    #[test]
    fn active_anchor_requirements_match_individual_coverage() -> Result<()> {
        let (image, first_anchor, newest_anchor) = two_batch_image()?;
        let discovery = discover_image(&image)?;
        let mut candidate = discovery.candidates[0].clone();
        let mut trailing_control_anchor = first_anchor;
        trailing_control_anchor.durable_end += FRAME_LEN_U64;
        let mut different_digest_anchor = newest_anchor;
        different_digest_anchor.prefix_digest[0] ^= 1;
        let mut later_physical_anchor = first_anchor;
        later_physical_anchor.durable_end = newest_anchor.durable_end + FRAME_LEN_U64;
        let mut later_timestamp_anchor = first_anchor;
        later_timestamp_anchor.last_commit_ts = newest_anchor.last_commit_ts + 1;
        let anchor_sets = [
            vec![first_anchor],
            vec![newest_anchor, first_anchor],
            vec![first_anchor, first_anchor, newest_anchor, newest_anchor],
            vec![first_anchor, trailing_control_anchor],
            vec![trailing_control_anchor, first_anchor, newest_anchor],
            vec![newest_anchor, different_digest_anchor],
            vec![newest_anchor, later_physical_anchor],
            vec![newest_anchor, later_timestamp_anchor],
        ];
        assert!(WalV7ActiveAnchorRequirements::new(&[]).is_err());

        for anchors in &anchor_sets {
            let requirements = WalV7ActiveAnchorRequirements::new(anchors)?;
            for ticket_end in 0..=3 {
                candidate.frontier.ticket_end = ticket_end;
                for durable_end in [
                    HEADER_LEN_U64,
                    first_anchor.durable_end,
                    trailing_control_anchor.durable_end,
                    newest_anchor.durable_end,
                    newest_anchor.durable_end + FRAME_LEN_U64,
                ] {
                    candidate.frontier.durable_end = durable_end;
                    for last_commit_ts in [0, 9, 10, 19, 20, 21] {
                        candidate.frontier.last_commit_ts = last_commit_ts;
                        for prefix_digest in [
                            first_anchor.prefix_digest,
                            newest_anchor.prefix_digest,
                            different_digest_anchor.prefix_digest,
                        ] {
                            candidate.frontier.prefix_digest = prefix_digest;
                            assert_eq!(
                                requirements.covers(&candidate),
                                anchors
                                    .iter()
                                    .all(|anchor| candidate_covers_anchor(&candidate, anchor)),
                                "coverage differs for {anchors:?} and {candidate:?}"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn active_recovery_keeps_same_ticket_physical_end_requirements() -> Result<()> {
        let (mut image, first_anchor, newest_anchor) = two_batch_image()?;
        let mut trailing_control_anchor = first_anchor;
        trailing_control_anchor.durable_end += FRAME_LEN_U64;
        let anchors = vec![trailing_control_anchor, first_anchor];
        let discovery = discover_image(&image)?;
        let mut reader = Cursor::new(image.as_slice());

        let selection = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: test_header().predecessor,
                durable_anchors: anchors.clone(),
            },
        )?;
        assert_eq!(selection.active_boundary, newest_anchor);

        image.truncate(usize::try_from(trailing_control_anchor.durable_end)?);
        let discovery = discover_image(&image)?;
        let mut reader = Cursor::new(image.as_slice());
        let verification = verify_frontier_candidate_prefixes_with_anchors(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery.candidates,
            &anchors,
            Some(test_header().predecessor),
        )?;
        assert_eq!(verification.verified_candidates.len(), 2);
        let error = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: test_header().predecessor,
                durable_anchors: anchors,
            },
        )
        .expect_err("an equal-ticket candidate cannot satisfy different physical anchor ends");
        assert!(error.to_string().contains("satisfies all durable anchors"));
        Ok(())
    }

    #[test]
    fn active_recovery_falls_back_only_above_the_exact_durable_anchor() -> Result<()> {
        let (mut image, first_anchor, newest_anchor) = two_batch_image()?;
        let second_data_start = first_anchor.durable_end + FRAME_LEN_U64;
        image[usize::try_from(second_data_start)?..usize::try_from(second_data_start + 64)?]
            .fill(0);
        let discovery = discover_image(&image)?;
        let mut reader = Cursor::new(image.as_slice());

        let selection = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: test_header().predecessor,
                durable_anchors: vec![first_anchor],
            },
        )?;

        assert_eq!(selection.active_boundary, first_anchor);
        assert!(selection.diagnostics.fallback_reason.is_some());
        assert!(selection.diagnostics.rejected_frontier_count > 0);
        assert_eq!(
            selection.diagnostics.discarded_physical_tail,
            Some((first_anchor.durable_end, u64::try_from(image.len())?))
        );

        let mut reader = Cursor::new(image.as_slice());
        let error = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: test_header().predecessor,
                durable_anchors: vec![newest_anchor],
            },
        )
        .expect_err("Active recovery cannot fall below a broken durable anchor");
        assert!(error.to_string().contains("durable anchor"));
        Ok(())
    }

    #[test]
    fn active_recovery_rejects_an_anchor_for_another_segment() -> Result<()> {
        let (image, mut anchor, _) = two_batch_image()?;
        anchor.incarnation = [0x55; 16];
        let discovery = discover_image(&image)?;
        let mut reader = Cursor::new(image.as_slice());

        let error = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: test_header().predecessor,
                durable_anchors: vec![anchor],
            },
        )
        .expect_err("a durable anchor cannot authorize a different WAL incarnation");

        assert!(error.to_string().contains("different WAL segment"));
        Ok(())
    }

    #[test]
    fn active_recovery_rejects_conflicting_anchor_and_predecessor_identity() -> Result<()> {
        let (image, anchor, newest_anchor) = two_batch_image()?;
        let discovery = discover_image(&image)?;
        let mut conflicting_anchor = anchor;
        conflicting_anchor.prefix_digest[0] ^= 0x01;
        let mut reader = Cursor::new(image.as_slice());

        let error = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: test_header().predecessor,
                durable_anchors: vec![newest_anchor, conflicting_anchor],
            },
        )
        .expect_err("Active recovery must validate the exact anchored logical prefix");
        assert!(error.to_string().contains("logical prefix digest mismatch"));

        let mut reader = Cursor::new(image.as_slice());
        let error = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: ChainAnchor::Genesis {
                    archive_epoch_id: ArchiveEpochId([0x77; 16]),
                },
                durable_anchors: vec![anchor],
            },
        )
        .expect_err("the durable source authority must bind the header predecessor");
        assert!(error.to_string().contains("authoritative segment identity"));
        Ok(())
    }

    #[test]
    fn immutable_recovery_requires_the_exact_terminal_image() -> Result<()> {
        let (image, _, active) = two_batch_image()?;
        let discovery = discover_image(&image)?;
        let terminal_offset = u64::try_from(image.len())? - FRAME_LEN_U64;
        let terminal = discovery
            .candidates
            .iter()
            .find(|candidate| candidate.frame_offset == terminal_offset)
            .context("test image has no terminal candidate")?;
        let boundary = ImmutableBoundary {
            active,
            sealed_end: u64::try_from(image.len())?,
            terminal_frontier_digest: terminal.frame_digest,
            wal_digest: Sha256::digest(&image).into(),
        };
        let mut reader = Cursor::new(image.as_slice());

        let selection = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Sealed {
                predecessor: test_header().predecessor,
                boundary,
            },
        )?;
        assert_eq!(selection.active_boundary, active);
        assert_eq!(selection.candidate.frame_offset, terminal_offset);
        assert_eq!(selection.diagnostics.discarded_physical_tail, None);

        let mut wrong_digest_boundary = boundary;
        wrong_digest_boundary.wal_digest[0] ^= 0x01;
        let mut reader = Cursor::new(image.as_slice());
        let error = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Sealing {
                predecessor: test_header().predecessor,
                boundary: wrong_digest_boundary,
            },
        )
        .expect_err("Sealing recovery must reject an immutable-image digest mismatch");
        assert!(error.to_string().contains("image digest mismatch"));

        let mut damaged_terminal_image = image.clone();
        damaged_terminal_image[usize::try_from(terminal_offset + 64)?] ^= 0x01;
        let damaged_discovery = discover_image(&damaged_terminal_image)?;
        let mut reader = Cursor::new(damaged_terminal_image.as_slice());
        let error = select_recovery_candidate(
            &mut reader,
            u64::try_from(damaged_terminal_image.len())?,
            &damaged_discovery,
            WalV7RecoveryAuthority::Sealed {
                predecessor: test_header().predecessor,
                boundary,
            },
        )
        .expect_err("immutable recovery must not fall back past a damaged terminal marker");
        assert!(error.to_string().contains("terminal FRONTIER"));

        let mut extended_image = image.clone();
        extended_image.resize(extended_image.len() + WAL_V7_FRAME_LEN, 0);
        let mut reader = Cursor::new(extended_image.as_slice());
        let error = select_recovery_candidate(
            &mut reader,
            u64::try_from(extended_image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Sealed {
                predecessor: test_header().predecessor,
                boundary,
            },
        )
        .expect_err("Sealed recovery must reject bytes outside its fixed image");
        assert!(error.to_string().contains("length differs"));
        Ok(())
    }

    #[test]
    fn immutable_recovery_accepts_the_generation_zero_empty_image() -> Result<()> {
        let TestWalImage {
            image,
            header_digest,
            frontier_zero,
            frontier_zero_digest,
        } = test_image()?;
        let active = boundary_for(frontier_zero, test_header());
        let boundary = ImmutableBoundary {
            active,
            sealed_end: u64::try_from(image.len())?,
            terminal_frontier_digest: frontier_zero_digest,
            wal_digest: Sha256::digest(&image).into(),
        };
        let discovery = discover_image(&image)?;
        let mut reader = Cursor::new(image.as_slice());

        let selection = select_recovery_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovery,
            WalV7RecoveryAuthority::Sealed {
                predecessor: test_header().predecessor,
                boundary,
            },
        )?;

        assert_eq!(selection.active_boundary, active);
        assert_eq!(selection.candidate.frontier.prefix_digest, header_digest);
        assert_eq!(selection.candidate.frame_offset, HEADER_LEN_U64);
        Ok(())
    }

    #[test]
    fn verifies_fragmented_batches_and_trailing_control_frames_in_one_pass() -> Result<()> {
        let TestWalImage {
            mut image,
            header_digest,
            frontier_zero,
            frontier_zero_digest: digest_zero,
        } = test_image()?;
        let header_bytes = image[..WAL_V7_HEADER_LEN].to_vec();
        let (first_data_end, first_batch) =
            append_data_batch(&mut image, 8192, header_digest, 0, 10, 100, b"first")?;
        let second_data_start = first_data_end;
        let (second_data_end, second_batch) = append_data_batch(
            &mut image,
            second_data_start,
            header_digest,
            1,
            20,
            101,
            &[0x9a; 5000],
        )?;
        let first_prefix_digest = logical_prefix_digest(&header_bytes, &[&first_batch]);
        let first_frontier_offset = second_data_end;
        let (frontier_one, digest_one) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: digest_zero,
                frame_offset: first_frontier_offset,
                ticket_end: 1,
                durable_end: first_data_end,
                last_commit_ts: 10,
                prefix_digest: first_prefix_digest,
            },
        )?;
        let second_prefix_digest =
            logical_prefix_digest(&header_bytes, &[&first_batch, &second_batch]);
        let second_frontier_offset = first_frontier_offset + FRAME_LEN_U64;
        let (frontier_two, _) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_one,
                previous_offset: first_frontier_offset,
                previous_digest: digest_one,
                frame_offset: second_frontier_offset,
                ticket_end: 2,
                durable_end: second_data_end.max(second_frontier_offset),
                last_commit_ts: 20,
                prefix_digest: second_prefix_digest,
            },
        )?;

        let verified = discover_and_verify(&image)?;

        assert_eq!(verified.verified_candidates.len(), 3);
        assert!(verified.rejected_candidates.is_empty());
        assert_eq!(verified.rejected_candidate_count, 0);
        assert_eq!(verified.verified_prefix_end, frontier_two.durable_end);
        assert!(verified.stopped_at.is_none());
        assert_eq!(
            verified.verified_candidates[0].frontier, frontier_two,
            "discovery order keeps the newest physical marker first"
        );
        Ok(())
    }

    #[test]
    fn accepts_a_frontier_before_corrupt_speculative_data_and_rejects_newer_prefixes() -> Result<()>
    {
        let TestWalImage {
            mut image,
            header_digest,
            frontier_zero,
            frontier_zero_digest: digest_zero,
        } = test_image()?;
        let header_bytes = image[..WAL_V7_HEADER_LEN].to_vec();
        let (data_end, batch) =
            append_data_batch(&mut image, 8192, header_digest, 0, 10, 100, b"durable")?;
        // A broken DATA slot follows the acknowledged prefix. F1 is physically
        // after it but names only D0, so the damaged speculative slot is excluded.
        image.resize(12_288, 0);
        let first_prefix_digest = logical_prefix_digest(&header_bytes, &[&batch]);
        let (frontier_one, digest_one) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: digest_zero,
                frame_offset: 16_384,
                ticket_end: 1,
                durable_end: data_end,
                last_commit_ts: 10,
                prefix_digest: first_prefix_digest,
            },
        )?;
        let (frontier_two, _) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_one,
                previous_offset: 16_384,
                previous_digest: digest_one,
                frame_offset: 24_576,
                ticket_end: 2,
                durable_end: 20_480,
                last_commit_ts: 20,
                prefix_digest: [0x77; 32],
            },
        )?;

        let verified = discover_and_verify(&image)?;

        assert_eq!(verified.verified_prefix_end, data_end);
        assert!(verified.stopped_at.is_some());
        assert!(
            verified
                .verified_candidates
                .iter()
                .any(|candidate| candidate.frontier == frontier_one)
        );
        assert!(
            !verified
                .verified_candidates
                .iter()
                .any(|candidate| candidate.frontier == frontier_two)
        );
        assert!(verified.rejected_candidate_count > 0);
        Ok(())
    }

    #[test]
    fn accepts_candidate_when_speculative_interval_contains_a_stray_frontier() -> Result<()> {
        let TestWalImage {
            mut image,
            header_digest,
            frontier_zero,
            frontier_zero_digest: digest_zero,
        } = test_image()?;
        let header_bytes = image[..WAL_V7_HEADER_LEN].to_vec();
        let (data_end, batch) =
            append_data_batch(&mut image, 8192, header_digest, 0, 10, 100, b"durable")?;
        let prefix_digest = logical_prefix_digest(&header_bytes, &[&batch]);

        // This sibling marker is a valid candidate on its own, but it lies at
        // durable_end and so is outside the later candidate's retained prefix.
        let (stray_frontier, _) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: digest_zero,
                frame_offset: data_end,
                ticket_end: 1,
                durable_end: data_end,
                last_commit_ts: 10,
                prefix_digest,
            },
        )?;
        let candidate_offset = data_end + FRAME_LEN_U64;
        let (candidate_frontier, _) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: digest_zero,
                frame_offset: candidate_offset,
                ticket_end: 1,
                durable_end: data_end,
                last_commit_ts: 10,
                prefix_digest,
            },
        )?;

        let verified = discover_and_verify(&image)?;

        assert!(verified.verified_candidates.iter().any(|candidate| {
            candidate.frame_offset == data_end && candidate.frontier == stray_frontier
        }));
        assert!(verified.verified_candidates.iter().any(|candidate| {
            candidate.frame_offset == candidate_offset && candidate.frontier == candidate_frontier
        }));
        assert!(verified.rejected_candidates.is_empty());
        assert_eq!(verified.rejected_candidate_count, 0);
        Ok(())
    }

    #[test]
    fn rejects_a_candidate_whose_retained_prefix_contains_a_frontier_fork() -> Result<()> {
        let TestWalImage {
            mut image,
            header_digest,
            frontier_zero,
            frontier_zero_digest: digest_zero,
        } = test_image()?;
        let header_bytes = image[..WAL_V7_HEADER_LEN].to_vec();
        let (first_data_end, first_batch) =
            append_data_batch(&mut image, 8192, header_digest, 0, 10, 100, b"first")?;
        let first_prefix_digest = logical_prefix_digest(&header_bytes, &[&first_batch]);
        let (frontier_one_a, digest_one_a) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: digest_zero,
                frame_offset: first_data_end,
                ticket_end: 1,
                durable_end: first_data_end,
                last_commit_ts: 10,
                prefix_digest: first_prefix_digest,
            },
        )?;
        let (frontier_one_b, _) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: digest_zero,
                frame_offset: first_data_end + FRAME_LEN_U64,
                ticket_end: 1,
                durable_end: first_data_end,
                last_commit_ts: 10,
                prefix_digest: first_prefix_digest,
            },
        )?;
        let (second_data_end, second_batch) = append_data_batch(
            &mut image,
            first_data_end + 2 * FRAME_LEN_U64,
            header_digest,
            1,
            20,
            101,
            b"second",
        )?;
        let second_prefix_digest =
            logical_prefix_digest(&header_bytes, &[&first_batch, &second_batch]);
        let forked_frontier_offset = first_data_end + 4 * FRAME_LEN_U64;
        let (frontier_two, _) = append_successor(
            &mut image,
            header_digest,
            SuccessorSpec {
                previous: frontier_one_a,
                previous_offset: first_data_end,
                previous_digest: digest_one_a,
                frame_offset: forked_frontier_offset,
                ticket_end: 2,
                durable_end: second_data_end.max(first_data_end + 2 * FRAME_LEN_U64),
                last_commit_ts: 20,
                prefix_digest: second_prefix_digest,
            },
        )?;

        let verified = discover_and_verify(&image)?;

        assert!(
            verified
                .verified_candidates
                .iter()
                .any(|candidate| candidate.frontier == frontier_one_a)
        );
        assert!(
            verified
                .verified_candidates
                .iter()
                .any(|candidate| candidate.frontier == frontier_one_b)
        );
        assert!(
            !verified
                .verified_candidates
                .iter()
                .any(|candidate| candidate.frontier == frontier_two)
        );
        assert!(verified.rejected_candidate_count > 0);
        Ok(())
    }

    #[test]
    fn discovers_valid_frontiers_after_bad_frames_and_reports_bad_marker() -> Result<()> {
        let mut image = vec![0; WAL_V7_HEADER_LEN];
        let (frontier_zero, digest_zero) = generation_zero()?;
        let frame_zero = encode_generation_zero_frontier(HEADER_DIGEST)?;
        install_frame(&mut image, HEADER_LEN_U64, &frame_zero)?;

        // The zero-filled slots at 8192, 16384, and 20480 model broken DATA,
        // a hole, and another broken DATA frame. Discovery must keep scanning.
        let (frame_one, frontier_one) = successor(
            frontier_zero,
            HEADER_LEN_U64,
            digest_zero,
            12_288,
            1,
            12_288,
            1,
        )?;
        let digest_one = frame_record_digest(&frame_one)?;
        install_frame(&mut image, 12_288, &frame_one)?;

        let (frame_two, frontier_two) =
            successor(frontier_one, 12_288, digest_one, 24_576, 2, 24_576, 2)?;
        install_frame(&mut image, 24_576, &frame_two)?;

        let (mut torn_frame, _) = successor(
            frontier_two,
            24_576,
            frame_record_digest(&frame_two)?,
            28_672,
            3,
            28_672,
            3,
        )?;
        torn_frame[60] ^= 0x01;
        install_frame(&mut image, 28_672, &torn_frame)?;
        image.extend_from_slice(&[1, 2, 3, 4, 5]);

        let mut reader = Cursor::new(image);
        let discovery = discover_frontier_candidates(&mut reader, 32_773, &HEADER_DIGEST)?;

        assert_eq!(
            discovery
                .candidates
                .iter()
                .map(|candidate| candidate.frame_offset)
                .collect::<Vec<_>>(),
            vec![24_576, 12_288, HEADER_LEN_U64]
        );
        assert_eq!(discovery.rejected_frontier_count, 1);
        assert_eq!(discovery.rejected_frontiers[0].frame_offset, 28_672);
        assert_eq!(discovery.scanned_frame_count, 7);
        assert_eq!(discovery.incomplete_tail_bytes, 5);

        Ok(())
    }

    #[test]
    fn partial_generation_zero_is_not_a_candidate() -> Result<()> {
        let image = vec![0; WAL_V7_HEADER_LEN + 23];
        let mut reader = Cursor::new(image);

        let discovery = discover_frontier_candidates(
            &mut reader,
            (WAL_V7_HEADER_LEN + 23) as u64,
            &HEADER_DIGEST,
        )?;

        assert!(discovery.candidates.is_empty());
        assert_eq!(discovery.scanned_frame_count, 0);
        assert_eq!(discovery.incomplete_tail_bytes, 23);

        Ok(())
    }

    #[test]
    fn discovery_rejects_images_above_the_hard_wal_cap_before_io() {
        let mut reader = Cursor::new(Vec::new());
        let oversized_len = MAX_WAL_FILE_SIZE + FRAME_LEN_U64;

        let error = discover_frontier_candidates(&mut reader, oversized_len, &HEADER_DIGEST)
            .expect_err("oversized v7 WAL images must be rejected before scanning");

        assert!(error.to_string().contains("1 GiB recovery limit"));
    }

    #[test]
    fn discovery_crosses_batch_boundary_without_skipping_or_repeating_frames() -> Result<()> {
        let mut image = vec![0; WAL_V7_HEADER_LEN];
        let (frontier_zero, digest_zero) = generation_zero()?;
        let frame_zero = encode_generation_zero_frontier(HEADER_DIGEST)?;
        install_frame(&mut image, HEADER_LEN_U64, &frame_zero)?;

        let far_offset = 70 * FRAME_LEN_U64;
        let (far_frame, _) = successor(
            frontier_zero,
            HEADER_LEN_U64,
            digest_zero,
            far_offset,
            1,
            2 * FRAME_LEN_U64,
            1,
        )?;
        install_frame(&mut image, far_offset, &far_frame)?;
        image.resize(71 * WAL_V7_FRAME_LEN, 0);

        let mut reader = Cursor::new(image);
        let discovery =
            discover_frontier_candidates(&mut reader, 71 * FRAME_LEN_U64, &HEADER_DIGEST)?;

        assert_eq!(discovery.scanned_frame_count, 70);
        assert_eq!(
            discovery
                .candidates
                .iter()
                .map(|candidate| candidate.frame_offset)
                .collect::<Vec<_>>(),
            vec![far_offset, HEADER_LEN_U64]
        );

        Ok(())
    }

    #[derive(Clone, Copy)]
    enum FailurePoint {
        Seek,
        Read,
    }

    struct FailingReader {
        failure: FailurePoint,
    }

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            match self.failure {
                FailurePoint::Read => Err(io::Error::other("injected read failure")),
                FailurePoint::Seek => Ok(0),
            }
        }
    }

    impl Seek for FailingReader {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            match self.failure {
                FailurePoint::Seek => Err(io::Error::other("injected seek failure")),
                FailurePoint::Read => match position {
                    SeekFrom::Start(position) => Ok(position),
                    _ => Err(io::Error::other("unexpected relative seek")),
                },
            }
        }
    }

    #[test]
    fn discovery_propagates_seek_errors() {
        let mut reader = FailingReader {
            failure: FailurePoint::Seek,
        };

        let error = discover_frontier_candidates(&mut reader, 2 * FRAME_LEN_U64, &HEADER_DIGEST)
            .expect_err("seek failure must fail discovery");

        assert!(error.to_string().contains("seek v7 WAL"));
    }

    #[test]
    fn discovery_propagates_read_errors() {
        let mut reader = FailingReader {
            failure: FailurePoint::Read,
        };

        let error = discover_frontier_candidates(&mut reader, 2 * FRAME_LEN_U64, &HEADER_DIGEST)
            .expect_err("read failure must fail discovery");

        assert!(error.to_string().contains("read v7 WAL"));
    }

    #[test]
    fn prefix_verification_propagates_read_errors() {
        let mut reader = FailingReader {
            failure: FailurePoint::Read,
        };

        let error = verify_frontier_candidate_prefixes(
            &mut reader,
            WAL_V7_HEADER_LEN as u64 + FRAME_LEN_U64,
            &[],
        )
        .expect_err("prefix read failure must fail verification");

        assert!(error.to_string().contains("read v7 WAL immutable header"));
    }
}
