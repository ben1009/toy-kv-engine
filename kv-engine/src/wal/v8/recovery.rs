//! Active v8 FRONTIER discovery and logical-prefix verification.
//!
//! Candidate discovery walks aligned physical frame slots without depending on
//! DATA parsing. Verification then scans the covered prefix in physical order,
//! validates every retained FRONTIER as one chain, and independently folds the
//! canonical logical batch digests.

use std::io::{Read, Seek, SeekFrom};

use anyhow::{Context, Result, ensure};

use crate::pitr::{
    RecordedAt, WalBatch, WalV5Limits, decode_wal_entry_stream, manifest::ActiveBoundary,
};

use super::{
    WAL_V8_HEADER_LEN as HEADER_LEN,
    codec::{
        WAL_V8_LOGICAL_BATCH_HEADER_LEN, WalV8DataFragmentHeader, WalV8FrameKind, WalV8Frontier,
        WalV8Header, WalV8LogicalBatchHeader, decode_frame_structural, decode_frontier_successor,
        encode_generation_zero_frontier, frame_record_digest, is_frontier_frame_header,
    },
    digest::{self, WalV8Digest, WalV8EntryHasher},
};

use super::codec::WAL_V8_FRAME_LEN as FRAME_LEN;

use crate::wal::MAX_WAL_FILE_SIZE;

const FRAME_LEN_U64: u64 = FRAME_LEN as u64;
const HEADER_LEN_U64: u64 = HEADER_LEN as u64;
const DISCOVERY_BATCH_FRAMES: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalV8FrontierCandidate {
    pub(crate) frame_offset: u64,
    pub(crate) frontier: WalV8Frontier,
    pub(crate) frame_digest: [u8; 32],
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalV8FrontierDiscovery {
    /// Structural candidates in descending physical-offset order.
    pub(crate) candidates: Vec<WalV8FrontierCandidate>,
    pub(crate) rejected_frontier_count: u64,
    pub(crate) scanned_frame_count: u64,
    pub(crate) incomplete_tail_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PrefixCheckpoint {
    data_start: u64,
    data_end: u64,
    last_commit_ts: u64,
    recorded_at: Option<RecordedAt>,
    prefix_digest: WalV8Digest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PrefixFrontier {
    frame_offset: u64,
    frontier: WalV8Frontier,
    frame_digest: [u8; 32],
}

/// An Active candidate validated against its entire covered prefix and all
/// supplied hard anchors. `batches` is truncated to the accepted ticket end.
pub(crate) struct WalV8RecoverySelection {
    pub(crate) header: WalV8Header,
    pub(crate) header_bytes: [u8; HEADER_LEN],
    pub(crate) physical_header_digest: [u8; 32],
    pub(crate) initial_prefix_digest: WalV8Digest,
    pub(crate) candidate: WalV8FrontierCandidate,
    pub(crate) active_boundary: ActiveBoundary,
    pub(crate) hard_anchors: Vec<ActiveBoundary>,
    pub(crate) batches: Vec<WalBatch>,
    pub(crate) last_frontier: WalV8Frontier,
    pub(crate) last_frontier_offset: u64,
    pub(crate) last_frontier_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8SealTicket {
    pub(crate) first_data_offset: u64,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
}

/// Strictly verified terminal boundary plus compact metadata for seal indexing.
/// It intentionally does not retain decoded key/value batches.
pub(crate) struct WalV8SealSelection {
    pub(crate) header: WalV8Header,
    pub(crate) candidate: WalV8FrontierCandidate,
    pub(crate) active_boundary: ActiveBoundary,
    pub(crate) tickets: Vec<WalV8SealTicket>,
}

/// Find structurally valid FRONTIER frames by scanning every aligned slot from
/// the physical tail. Corrupt DATA, holes, and zero-filled frames do not stop
/// discovery because FRONTIER candidates are independent recovery certificates.
pub(crate) fn discover_frontier_candidates<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    physical_header_digest: &[u8; 32],
    initial_prefix_digest: &WalV8Digest,
) -> Result<WalV8FrontierDiscovery> {
    ensure!(
        file_len >= HEADER_LEN_U64,
        "v8 WAL is shorter than its immutable header"
    );
    ensure!(
        file_len <= MAX_WAL_FILE_SIZE,
        "v8 WAL exceeds the maximum recovery size"
    );

    let complete_end = file_len - file_len % FRAME_LEN_U64;
    let mut discovery = WalV8FrontierDiscovery {
        incomplete_tail_bytes: file_len - complete_end,
        ..WalV8FrontierDiscovery::default()
    };
    let Some(mut last_offset) = complete_end.checked_sub(FRAME_LEN_U64) else {
        return Ok(discovery);
    };
    if last_offset < HEADER_LEN_U64 {
        return Ok(discovery);
    }

    loop {
        let available = (last_offset - HEADER_LEN_U64) / FRAME_LEN_U64 + 1;
        let count = available.min(DISCOVERY_BATCH_FRAMES as u64);
        let bytes_len = usize::try_from(
            count
                .checked_mul(FRAME_LEN_U64)
                .context("v8 discovery batch length overflows")?,
        )?;
        let first_offset = last_offset
            .checked_add(FRAME_LEN_U64)
            .and_then(|end| end.checked_sub(bytes_len as u64))
            .context("v8 discovery batch offset overflows")?;
        let mut bytes = vec![0; bytes_len];
        reader
            .seek(SeekFrom::Start(first_offset))
            .with_context(|| format!("seek v8 WAL to {first_offset} during discovery"))?;
        reader
            .read_exact(&mut bytes)
            .with_context(|| format!("read v8 WAL discovery frames at {first_offset}"))?;

        for frame_index in (0..usize::try_from(count)?).rev() {
            discovery.scanned_frame_count = discovery
                .scanned_frame_count
                .checked_add(1)
                .context("v8 discovery frame count overflows")?;
            let start = frame_index
                .checked_mul(FRAME_LEN)
                .context("v8 discovery frame index overflows")?;
            let frame_bytes = &bytes[start..start + FRAME_LEN];
            if !is_frontier_frame_header(frame_bytes) {
                continue;
            }

            let frame_offset = first_offset
                .checked_add(u64::try_from(start)?)
                .context("v8 discovered frame offset overflows")?;
            if let Ok(frame) = decode_frame_structural(
                frame_bytes,
                frame_offset,
                physical_header_digest,
                initial_prefix_digest,
            ) && frame.kind == WalV8FrameKind::Frontier
            {
                let frontier = WalV8Frontier::decode_body(&frame.body)?;
                discovery
                    .candidates
                    .try_reserve(1)
                    .context("reserve v8 FRONTIER candidate")?;
                discovery.candidates.push(WalV8FrontierCandidate {
                    frame_offset,
                    frontier,
                    frame_digest: frame_record_digest(frame_bytes)?,
                });
            } else {
                discovery.rejected_frontier_count = discovery
                    .rejected_frontier_count
                    .checked_add(1)
                    .context("v8 rejected FRONTIER count overflows")?;
            }
        }

        if first_offset == HEADER_LEN_U64 {
            break;
        }
        last_offset = first_offset
            .checked_sub(FRAME_LEN_U64)
            .context("v8 discovery offset underflows")?;
    }

    Ok(discovery)
}

/// Verify discovered candidates and select the newest Active boundary that
/// preserves every hard anchor. The caller must keep the source image stable
/// across discovery and verification.
pub(crate) fn select_active_candidate<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    discovery: &WalV8FrontierDiscovery,
    anchors: &[ActiveBoundary],
    expected_predecessor: crate::pitr::ChainAnchor,
    limits: WalV5Limits,
) -> Result<WalV8RecoverySelection> {
    let (selection, _) = select_active_candidate_inner(
        reader,
        file_len,
        discovery,
        anchors,
        expected_predecessor,
        limits,
        false,
    )?;
    Ok(selection)
}

/// Verify a frozen v8 boundary and return only its compact ticket index.
pub(crate) fn select_active_candidate_for_seal<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    discovery: &WalV8FrontierDiscovery,
    anchors: &[ActiveBoundary],
    expected_predecessor: crate::pitr::ChainAnchor,
    limits: WalV5Limits,
) -> Result<WalV8SealSelection> {
    let (selection, tickets) = select_active_candidate_inner(
        reader,
        file_len,
        discovery,
        anchors,
        expected_predecessor,
        limits,
        true,
    )?;
    Ok(WalV8SealSelection {
        header: selection.header,
        candidate: selection.candidate,
        active_boundary: selection.active_boundary,
        tickets,
    })
}

fn select_active_candidate_inner<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    discovery: &WalV8FrontierDiscovery,
    anchors: &[ActiveBoundary],
    expected_predecessor: crate::pitr::ChainAnchor,
    limits: WalV5Limits,
    collect_seal_index: bool,
) -> Result<(WalV8RecoverySelection, Vec<WalV8SealTicket>)> {
    ensure!(
        !anchors.is_empty(),
        "v8 Active recovery requires a durable anchor"
    );
    ensure!(
        file_len >= HEADER_LEN_U64 + FRAME_LEN_U64,
        "v8 Active WAL is missing generation-zero FRONTIER"
    );
    let limits = limits.validate()?;

    reader
        .seek(SeekFrom::Start(0))
        .context("seek v8 Active WAL header")?;
    let mut header_bytes = [0; HEADER_LEN];
    reader
        .read_exact(&mut header_bytes)
        .context("read v8 Active WAL header")?;
    let header = WalV8Header::decode(&header_bytes)?;
    ensure!(
        header.encode()? == header_bytes,
        "v8 Active WAL header is not canonical"
    );
    ensure!(
        header.predecessor == expected_predecessor,
        "v8 WAL predecessor differs from authoritative segment identity"
    );
    for anchor in anchors {
        anchor.validate()?;
        ensure!(
            anchor.timeline_id == header.timeline_id.0
                && anchor.archive_epoch_id == header.archive_epoch_id.0
                && anchor.segment_id == header.segment_id.0
                && anchor.incarnation == header.incarnation,
            "v8 Active durable anchor identifies a different segment"
        );
        ensure!(
            anchor.durable_end <= file_len,
            "v8 Active anchor exceeds the WAL image"
        );
    }

    let physical_header_digest = header.physical_digest()?;
    let initial_prefix_digest = header.initial_prefix_digest()?;
    let generation_zero_bytes = read_frame(reader, HEADER_LEN_U64)?;
    let generation_zero_frame = decode_frame_structural(
        &generation_zero_bytes,
        HEADER_LEN_U64,
        &physical_header_digest,
        &initial_prefix_digest,
    )?;
    ensure!(
        generation_zero_frame.kind == WalV8FrameKind::Frontier,
        "v8 initial slot is not a FRONTIER"
    );
    let generation_zero = WalV8Frontier::decode_body(&generation_zero_frame.body)?;
    generation_zero.validate_generation_zero(&initial_prefix_digest, HEADER_LEN_U64)?;
    let generation_zero_canonical =
        encode_generation_zero_frontier(physical_header_digest, initial_prefix_digest)?;
    ensure!(
        generation_zero_bytes == generation_zero_canonical,
        "v8 generation-zero FRONTIER is not canonical"
    );
    let generation_zero_digest = frame_record_digest(&generation_zero_bytes)?;

    let max_prefix_end = discovery
        .candidates
        .iter()
        .map(|candidate| candidate.frontier.durable_end)
        .chain(anchors.iter().map(|anchor| anchor.durable_end))
        .max()
        .unwrap_or(HEADER_LEN_U64);
    ensure!(
        max_prefix_end <= file_len && max_prefix_end.is_multiple_of(FRAME_LEN_U64),
        "v8 candidate prefix boundary exceeds the aligned WAL image"
    );

    let mut checkpoints = Vec::new();
    checkpoints
        .try_reserve(1)
        .context("reserve v8 initial checkpoint")?;
    checkpoints.push(PrefixCheckpoint {
        data_start: HEADER_LEN_U64,
        data_end: HEADER_LEN_U64,
        last_commit_ts: 0,
        recorded_at: None,
        prefix_digest: initial_prefix_digest,
    });
    let mut frontiers = Vec::new();
    frontiers
        .try_reserve(1)
        .context("reserve v8 generation-zero metadata")?;
    frontiers.push(PrefixFrontier {
        frame_offset: HEADER_LEN_U64,
        frontier: generation_zero,
        frame_digest: generation_zero_digest,
    });
    let mut cursor = HEADER_LEN_U64 + FRAME_LEN_U64;
    let mut last_recorded_at = None;
    let mut stopped_at: Option<(u64, String)> = None;

    while cursor < max_prefix_end {
        let frame_offset = cursor;
        // A failed read cannot authorize fallback to an older candidate.
        // Only successfully read, invalid bytes are treated as corruption.
        let frame_bytes = read_frame(reader, frame_offset)?;
        let frame = match decode_frame_structural(
            &frame_bytes,
            frame_offset,
            &physical_header_digest,
            &initial_prefix_digest,
        ) {
            Ok(frame) => frame,
            Err(error) => {
                stopped_at = Some((frame_offset, format!("{error:#}")));
                break;
            }
        };

        match frame.kind {
            WalV8FrameKind::Data => {
                let (first_fragment, first_payload) =
                    match WalV8DataFragmentHeader::decode_body(&frame.body) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            stopped_at = Some((frame_offset, format!("{error:#}")));
                            break;
                        }
                    };
                let expected_ticket = u64::try_from(checkpoints.len() - 1)?;
                if first_fragment.segment_ticket != expected_ticket
                    || first_fragment.fragment_index != 0
                {
                    stopped_at = Some((
                        frame_offset,
                        "v8 DATA ticket order or first fragment is invalid".into(),
                    ));
                    break;
                }
                let batch_len = match usize::try_from(first_fragment.batch_bytes) {
                    Ok(length) => length,
                    Err(error) => {
                        stopped_at = Some((frame_offset, error.to_string()));
                        break;
                    }
                };
                let max_batch_len = WAL_V8_LOGICAL_BATCH_HEADER_LEN
                    .checked_add(limits.max_batch_data_bytes)
                    .context("v8 maximum logical batch length overflows")?;
                if batch_len > max_batch_len {
                    stopped_at = Some((
                        frame_offset,
                        "v8 DATA batch exceeds configured byte limit".into(),
                    ));
                    break;
                }
                let frame_count = u64::from(first_fragment.fragment_count);
                let batch_end = match frame_count
                    .checked_mul(FRAME_LEN_U64)
                    .and_then(|span| frame_offset.checked_add(span))
                {
                    Some(end) if end <= max_prefix_end => end,
                    _ => {
                        stopped_at = Some((
                            frame_offset,
                            "v8 DATA batch extends beyond verified prefix".into(),
                        ));
                        break;
                    }
                };
                let mut logical_bytes = Vec::new();
                if let Err(error) = logical_bytes.try_reserve_exact(batch_len) {
                    return Err(error).context("reserve v8 logical batch recovery buffer");
                }
                logical_bytes.extend_from_slice(first_payload);
                let mut fragment_error = None;
                for fragment_index in 1..usize::try_from(first_fragment.fragment_count)? {
                    let fragment_offset = frame_offset
                        .checked_add(
                            u64::try_from(fragment_index)?
                                .checked_mul(FRAME_LEN_U64)
                                .context("v8 fragment offset overflows")?,
                        )
                        .context("v8 fragment offset overflows")?;
                    // Fragment I/O failures are fatal too; malformed bytes
                    // below may reject the candidate and permit fallback.
                    let bytes = read_frame(reader, fragment_offset)?;
                    let decoded = match decode_frame_structural(
                        &bytes,
                        fragment_offset,
                        &physical_header_digest,
                        &initial_prefix_digest,
                    ) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            fragment_error = Some(error);
                            break;
                        }
                    };
                    if decoded.kind != WalV8FrameKind::Data {
                        fragment_error = Some(anyhow::anyhow!("v8 DATA batch is interleaved"));
                        break;
                    }
                    let (fragment, payload) =
                        match WalV8DataFragmentHeader::decode_body(&decoded.body) {
                            Ok(value) => value,
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
                        fragment_error = Some(anyhow::anyhow!("v8 DATA fragments disagree"));
                        break;
                    }
                    logical_bytes.extend_from_slice(payload);
                }
                if let Some(error) = fragment_error {
                    stopped_at = Some((frame_offset, format!("{error:#}")));
                    break;
                }
                if logical_bytes.len() != batch_len || batch_len < WAL_V8_LOGICAL_BATCH_HEADER_LEN {
                    stopped_at = Some((
                        frame_offset,
                        "v8 DATA fragments have an invalid total length".into(),
                    ));
                    break;
                }

                let entry_bytes = &logical_bytes[WAL_V8_LOGICAL_BATCH_HEADER_LEN..];
                let logical_header = match WalV8LogicalBatchHeader::decode(
                    &logical_bytes[..WAL_V8_LOGICAL_BATCH_HEADER_LEN],
                    entry_bytes,
                    first_fragment.segment_ticket,
                    first_fragment.batch_bytes,
                    limits,
                ) {
                    Ok(header) => header,
                    Err(error) => {
                        stopped_at = Some((frame_offset, format!("{error:#}")));
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
                    entry_bytes,
                    limits,
                ) {
                    Ok(batch) => batch,
                    Err(error) => {
                        stopped_at = Some((frame_offset, format!("{error:#}")));
                        break;
                    }
                };
                let previous_checkpoint = *checkpoints.last().expect("initial checkpoint exists");
                if batch.commit_ts <= previous_checkpoint.last_commit_ts
                    || last_recorded_at.is_some_and(|previous| batch.recorded_at < previous)
                {
                    stopped_at = Some((
                        frame_offset,
                        "v8 commit timestamps or recorded times are out of order".into(),
                    ));
                    break;
                }
                let mut entry_hasher = WalV8EntryHasher::new(u64::try_from(entry_bytes.len())?);
                if let Err(error) = entry_hasher.update(entry_bytes) {
                    stopped_at = Some((frame_offset, format!("{error:#}")));
                    break;
                }
                let entry_digest = entry_hasher.finish()?;
                let logical_header_bytes: &[u8; WAL_V8_LOGICAL_BATCH_HEADER_LEN] =
                    logical_bytes[..WAL_V8_LOGICAL_BATCH_HEADER_LEN].try_into()?;
                let batch_digest = digest::batch_digest(logical_header_bytes, &entry_digest);
                let prefix_digest = digest::advance_prefix(
                    &previous_checkpoint.prefix_digest,
                    expected_ticket,
                    u64::try_from(batch_len)?,
                    &batch_digest,
                );
                checkpoints
                    .try_reserve(1)
                    .context("grow v8 prefix checkpoints")?;
                checkpoints.push(PrefixCheckpoint {
                    data_start: frame_offset,
                    data_end: batch_end,
                    last_commit_ts: batch.commit_ts,
                    recorded_at: Some(batch.recorded_at),
                    prefix_digest,
                });
                last_recorded_at = Some(batch.recorded_at);
                cursor = batch_end;
            }
            WalV8FrameKind::Frontier => {
                let frontier = match WalV8Frontier::decode_body(&frame.body) {
                    Ok(frontier) => frontier,
                    Err(error) => {
                        stopped_at = Some((frame_offset, format!("{error:#}")));
                        break;
                    }
                };
                let predecessor = *frontiers.last().expect("generation zero frontier exists");
                let frontier_digest = frame_record_digest(&frame_bytes)?;
                if let Err(error) =
                    validate_frontier_prefix(frontier, frame_offset, predecessor, &checkpoints)
                {
                    stopped_at = Some((frame_offset, format!("{error:#}")));
                    break;
                }
                if let Err(error) = decode_frontier_successor(
                    &frame_bytes,
                    frame_offset,
                    &physical_header_digest,
                    &initial_prefix_digest,
                    predecessor.frontier,
                    predecessor.frame_offset,
                    &predecessor.frame_digest,
                ) {
                    stopped_at = Some((frame_offset, format!("{error:#}")));
                    break;
                }
                frontiers.try_reserve(1).context("grow v8 FRONTIER chain")?;
                frontiers.push(PrefixFrontier {
                    frame_offset,
                    frontier,
                    frame_digest: frontier_digest,
                });
                cursor = cursor
                    .checked_add(FRAME_LEN_U64)
                    .context("v8 FRONTIER offset overflows")?;
            }
        }
    }

    for anchor in anchors {
        if anchor.durable_end > cursor {
            anyhow::bail!(
                "v8 hard anchor at ticket {} extends beyond verified prefix{}",
                anchor.ticket_end,
                stopped_at
                    .as_ref()
                    .map_or_else(String::new, |(offset, reason)| format!(
                        " stopped at {offset}: {reason}"
                    ))
            );
        }
        validate_anchor_prefix(*anchor, &checkpoints, &frontiers)?;
    }

    let mut eligible = Vec::new();
    for candidate in &discovery.candidates {
        if candidate.frontier.durable_end > cursor {
            continue;
        }
        if validate_candidate(
            candidate,
            &physical_header_digest,
            &initial_prefix_digest,
            &frontiers,
            &checkpoints,
        )
        .is_err()
        {
            continue;
        }
        if anchors
            .iter()
            .all(|anchor| candidate_covers_anchor(candidate, anchor))
        {
            eligible.push(candidate.clone());
        }
    }
    let candidate = eligible
        .into_iter()
        .max_by_key(|candidate| candidate.frame_offset)
        .context("no fully verified v8 FRONTIER satisfies all durable anchors")?;

    let mut hard_anchors = Vec::new();
    hard_anchors
        .try_reserve_exact(anchors.len())
        .context("reserve v8 Active recovery anchors")?;
    hard_anchors.extend_from_slice(anchors);

    let ticket_end = usize::try_from(candidate.frontier.ticket_end)?;
    ensure!(
        ticket_end < checkpoints.len(),
        "v8 candidate ticket end exceeds verified DATA"
    );
    let mut seal_tickets = Vec::new();
    if collect_seal_index {
        seal_tickets
            .try_reserve_exact(ticket_end)
            .context("reserve v8 seal ticket index")?;
        for checkpoint in checkpoints.iter().skip(1).take(ticket_end) {
            seal_tickets.push(WalV8SealTicket {
                first_data_offset: checkpoint.data_start,
                commit_ts: checkpoint.last_commit_ts,
                recorded_at: checkpoint
                    .recorded_at
                    .context("selected v8 batch has no recorded time")?,
            });
        }
    }
    let batches = if collect_seal_index {
        Vec::new()
    } else {
        decode_selected_batches(
            reader,
            &checkpoints,
            ticket_end,
            candidate.frontier,
            &physical_header_digest,
            &initial_prefix_digest,
            limits,
        )?
    };
    let active_boundary = ActiveBoundary {
        timeline_id: header.timeline_id.0,
        archive_epoch_id: header.archive_epoch_id.0,
        segment_id: header.segment_id.0,
        incarnation: header.incarnation,
        ticket_end: candidate.frontier.ticket_end,
        durable_end: candidate.frontier.durable_end,
        last_commit_ts: candidate.frontier.last_commit_ts,
        prefix_digest: candidate.frontier.prefix_digest,
    };
    active_boundary.validate()?;

    let predecessor = if candidate.frontier.ticket_end == 0 {
        frontiers[0]
    } else {
        retained_frontier(&frontiers, candidate.frontier.durable_end)?
    };
    Ok((
        WalV8RecoverySelection {
            header,
            header_bytes,
            physical_header_digest,
            initial_prefix_digest,
            candidate,
            active_boundary,
            hard_anchors,
            batches,
            last_frontier: predecessor.frontier,
            last_frontier_offset: predecessor.frame_offset,
            last_frontier_digest: predecessor.frame_digest,
        },
        seal_tickets,
    ))
}

/// Decode only batches covered by the selected frontier. The verification scan
/// above retains compact prefix checkpoints rather than every decoded batch,
/// so valid speculative tails rejected by Active fallback do not accumulate
/// as a second full WAL-sized in-memory copy.
fn decode_selected_batches<R: Read + Seek>(
    reader: &mut R,
    checkpoints: &[PrefixCheckpoint],
    ticket_end: usize,
    frontier: WalV8Frontier,
    physical_header_digest: &[u8; 32],
    initial_prefix_digest: &WalV8Digest,
    limits: WalV5Limits,
) -> Result<Vec<WalBatch>> {
    let mut batches = Vec::new();
    batches
        .try_reserve_exact(ticket_end)
        .context("reserve selected v8 recovery batches")?;
    let mut prefix_digest = *initial_prefix_digest;

    for ticket in 0..ticket_end {
        let checkpoint = checkpoints
            .get(ticket + 1)
            .context("selected v8 ticket has no verified checkpoint")?;
        let (logical_bytes, batch) = decode_batch_at(
            reader,
            checkpoint.data_start,
            checkpoint.data_end,
            u64::try_from(ticket)?,
            physical_header_digest,
            initial_prefix_digest,
            limits,
        )?;
        ensure!(
            batch.commit_ts == checkpoint.last_commit_ts,
            "selected v8 batch commit timestamp changed after validation"
        );
        let entry_bytes = &logical_bytes[WAL_V8_LOGICAL_BATCH_HEADER_LEN..];
        let mut entry_hasher = WalV8EntryHasher::new(u64::try_from(entry_bytes.len())?);
        entry_hasher.update(entry_bytes)?;
        let entry_digest = entry_hasher.finish()?;
        let logical_header: &[u8; WAL_V8_LOGICAL_BATCH_HEADER_LEN] =
            logical_bytes[..WAL_V8_LOGICAL_BATCH_HEADER_LEN].try_into()?;
        let batch_digest = digest::batch_digest(logical_header, &entry_digest);
        prefix_digest = digest::advance_prefix(
            &prefix_digest,
            u64::try_from(ticket)?,
            u64::try_from(logical_bytes.len())?,
            &batch_digest,
        );
        ensure!(
            prefix_digest == checkpoint.prefix_digest,
            "selected v8 batch prefix digest changed after validation"
        );
        batches.push(batch);
    }

    let last_checkpoint = checkpoints
        .get(ticket_end)
        .context("selected v8 frontier has no matching checkpoint")?;
    ensure!(
        prefix_digest == frontier.prefix_digest
            && last_checkpoint.prefix_digest == frontier.prefix_digest
            && last_checkpoint.last_commit_ts == frontier.last_commit_ts,
        "selected v8 batches do not reproduce the accepted FRONTIER"
    );
    Ok(batches)
}

fn decode_batch_at<R: Read + Seek>(
    reader: &mut R,
    frame_offset: u64,
    expected_end: u64,
    expected_ticket: u64,
    physical_header_digest: &[u8; 32],
    initial_prefix_digest: &WalV8Digest,
    limits: WalV5Limits,
) -> Result<(Vec<u8>, WalBatch)> {
    let first_bytes = read_frame(reader, frame_offset)?;
    let first = decode_frame_structural(
        &first_bytes,
        frame_offset,
        physical_header_digest,
        initial_prefix_digest,
    )?;
    ensure!(
        first.kind == WalV8FrameKind::Data,
        "selected v8 ticket is not DATA"
    );
    let (first_fragment, first_payload) = WalV8DataFragmentHeader::decode_body(&first.body)?;
    ensure!(
        first_fragment.segment_ticket == expected_ticket && first_fragment.fragment_index == 0,
        "selected v8 DATA ticket order or first fragment changed after validation"
    );

    let batch_len = usize::try_from(first_fragment.batch_bytes)?;
    let max_batch_len = WAL_V8_LOGICAL_BATCH_HEADER_LEN
        .checked_add(limits.max_batch_data_bytes)
        .context("v8 maximum logical batch length overflows")?;
    ensure!(
        batch_len >= WAL_V8_LOGICAL_BATCH_HEADER_LEN && batch_len <= max_batch_len,
        "selected v8 DATA batch length is outside configured limits"
    );
    let frame_count = u64::from(first_fragment.fragment_count);
    let actual_end = frame_count
        .checked_mul(FRAME_LEN_U64)
        .and_then(|span| frame_offset.checked_add(span))
        .context("selected v8 DATA batch end overflows")?;
    ensure!(
        actual_end == expected_end,
        "selected v8 DATA batch range changed after validation"
    );

    let mut logical_bytes = Vec::new();
    logical_bytes
        .try_reserve_exact(batch_len)
        .context("reserve selected v8 logical batch")?;
    logical_bytes.extend_from_slice(first_payload);
    for fragment_index in 1..usize::try_from(first_fragment.fragment_count)? {
        let fragment_offset = frame_offset
            .checked_add(
                u64::try_from(fragment_index)?
                    .checked_mul(FRAME_LEN_U64)
                    .context("selected v8 fragment offset overflows")?,
            )
            .context("selected v8 fragment offset overflows")?;
        let bytes = read_frame(reader, fragment_offset)?;
        let frame = decode_frame_structural(
            &bytes,
            fragment_offset,
            physical_header_digest,
            initial_prefix_digest,
        )?;
        ensure!(
            frame.kind == WalV8FrameKind::Data,
            "selected v8 DATA batch is interleaved"
        );
        let (fragment, payload) = WalV8DataFragmentHeader::decode_body(&frame.body)?;
        ensure!(
            fragment.segment_ticket == first_fragment.segment_ticket
                && fragment.fragment_index == u32::try_from(fragment_index)?
                && fragment.fragment_count == first_fragment.fragment_count
                && fragment.batch_bytes == first_fragment.batch_bytes,
            "selected v8 DATA fragments disagree after validation"
        );
        logical_bytes.extend_from_slice(payload);
    }
    ensure!(
        logical_bytes.len() == batch_len,
        "selected v8 DATA fragments have an invalid total length"
    );

    let entry_bytes = &logical_bytes[WAL_V8_LOGICAL_BATCH_HEADER_LEN..];
    let logical_header = WalV8LogicalBatchHeader::decode(
        &logical_bytes[..WAL_V8_LOGICAL_BATCH_HEADER_LEN],
        entry_bytes,
        expected_ticket,
        first_fragment.batch_bytes,
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
        entry_bytes,
        limits,
    )?;
    Ok((logical_bytes, batch))
}

fn validate_frontier_prefix(
    frontier: WalV8Frontier,
    frame_offset: u64,
    predecessor: PrefixFrontier,
    checkpoints: &[PrefixCheckpoint],
) -> Result<()> {
    let ticket_end = usize::try_from(frontier.ticket_end)?;
    let checkpoint = checkpoints
        .get(ticket_end)
        .context("v8 FRONTIER ticket end exceeds verified DATA")?;
    ensure!(
        frontier.last_commit_ts == checkpoint.last_commit_ts
            && frontier.prefix_digest == checkpoint.prefix_digest,
        "v8 FRONTIER timestamp or logical prefix digest mismatch"
    );
    let predecessor_end = predecessor
        .frame_offset
        .checked_add(FRAME_LEN_U64)
        .context("v8 predecessor frame end overflows")?;
    let expected_durable_end = checkpoint.data_end.max(predecessor_end);
    if let Some(next_batch) = checkpoints.get(ticket_end + 1) {
        ensure!(
            next_batch.data_start >= frontier.durable_end,
            "v8 FRONTIER durable_end splits a later DATA ticket"
        );
    }
    ensure!(
        frontier.durable_end == expected_durable_end,
        "v8 FRONTIER durable_end does not match its DATA and control prefix"
    );
    ensure!(
        predecessor_end <= frontier.durable_end && frontier.durable_end <= frame_offset,
        "v8 FRONTIER has invalid physical prefix bounds"
    );
    Ok(())
}

fn validate_candidate(
    candidate: &WalV8FrontierCandidate,
    physical_header_digest: &[u8; 32],
    initial_prefix_digest: &WalV8Digest,
    frontiers: &[PrefixFrontier],
    checkpoints: &[PrefixCheckpoint],
) -> Result<()> {
    if candidate.frontier.generation == 0 {
        ensure!(
            candidate.frame_offset == HEADER_LEN_U64
                && candidate.frontier.ticket_end == 0
                && candidate.frontier.durable_end == HEADER_LEN_U64
                && candidate.frontier.prefix_digest == *initial_prefix_digest,
            "invalid v8 generation-zero candidate"
        );
        return Ok(());
    }
    let predecessor = retained_frontier(frontiers, candidate.frontier.durable_end)?;
    validate_frontier_prefix(
        candidate.frontier,
        candidate.frame_offset,
        predecessor,
        checkpoints,
    )?;
    let encoded = super::codec::encode_frontier_successor(
        candidate.frontier,
        predecessor.frontier,
        predecessor.frame_offset,
        &predecessor.frame_digest,
        candidate.frame_offset,
        *physical_header_digest,
        *initial_prefix_digest,
    )?;
    ensure!(
        frame_record_digest(&encoded)? == candidate.frame_digest,
        "v8 candidate FRONTIER differs from its canonical predecessor chain"
    );
    Ok(())
}

fn validate_anchor_prefix(
    anchor: ActiveBoundary,
    checkpoints: &[PrefixCheckpoint],
    frontiers: &[PrefixFrontier],
) -> Result<()> {
    let ticket_end = usize::try_from(anchor.ticket_end)?;
    let checkpoint = checkpoints
        .get(ticket_end)
        .context("v8 Active anchor ticket end exceeds verified DATA")?;
    ensure!(
        checkpoint.last_commit_ts == anchor.last_commit_ts
            && checkpoint.prefix_digest == anchor.prefix_digest,
        "v8 Active anchor timestamp or logical prefix digest mismatch"
    );
    if ticket_end == 0 {
        ensure!(
            anchor.durable_end == HEADER_LEN_U64,
            "empty v8 Active anchor has a noncanonical durable boundary"
        );
        return Ok(());
    }

    let predecessor = retained_frontier(frontiers, anchor.durable_end)?;
    let predecessor_end = predecessor
        .frame_offset
        .checked_add(FRAME_LEN_U64)
        .context("v8 Active anchor predecessor end overflows")?;
    if let Some(next_batch) = checkpoints.get(ticket_end + 1) {
        ensure!(
            next_batch.data_start >= anchor.durable_end,
            "v8 Active anchor splits a later DATA ticket"
        );
    }
    ensure!(
        anchor.durable_end == checkpoint.data_end.max(predecessor_end),
        "v8 Active anchor does not match its exact physical prefix"
    );
    Ok(())
}

fn candidate_covers_anchor(candidate: &WalV8FrontierCandidate, anchor: &ActiveBoundary) -> bool {
    let frontier = candidate.frontier;
    if frontier.ticket_end < anchor.ticket_end
        || frontier.durable_end < anchor.durable_end
        || frontier.last_commit_ts < anchor.last_commit_ts
    {
        return false;
    }
    if frontier.ticket_end == anchor.ticket_end {
        frontier.durable_end == anchor.durable_end
            && frontier.last_commit_ts == anchor.last_commit_ts
            && frontier.prefix_digest == anchor.prefix_digest
    } else {
        frontier.durable_end > anchor.durable_end && frontier.last_commit_ts > anchor.last_commit_ts
    }
}

fn retained_frontier(frontiers: &[PrefixFrontier], durable_end: u64) -> Result<PrefixFrontier> {
    let count = frontiers.partition_point(|frontier| {
        frontier
            .frame_offset
            .checked_add(FRAME_LEN_U64)
            .is_some_and(|end| end <= durable_end)
    });
    ensure!(count > 0, "v8 retained prefix omits generation zero");
    Ok(frontiers[count - 1])
}

fn read_frame<R: Read + Seek>(reader: &mut R, offset: u64) -> Result<[u8; FRAME_LEN]> {
    let mut bytes = [0; FRAME_LEN];
    reader
        .seek(SeekFrom::Start(offset))
        .with_context(|| format!("seek v8 WAL frame at {offset}"))?;
    reader
        .read_exact(&mut bytes)
        .with_context(|| format!("read v8 WAL frame at {offset}"))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{self, Cursor, Read, Seek, SeekFrom},
        os::unix::fs::MetadataExt,
    };

    use anyhow::Result;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::pitr::{
        ArchiveEpochId, LIVE_WAL_V5_LIMITS, SegmentId, TimelineId, WalBatch, WalEntry,
        encode_v5_batch,
    };
    use crate::wal::v8::codec::{
        WAL_V8_DATA_FRAGMENT_PAYLOAD_CAPACITY, WalV8DataFragmentHeader, data_fragment_count,
        encode_data_frame, encode_frontier_successor, encode_generation_zero_frontier,
    };

    type EmptyImage = (Vec<u8>, [u8; 32], WalV8Frontier, [u8; 32]);

    fn test_header() -> WalV8Header {
        let archive_epoch_id = ArchiveEpochId([0x22; 16]);
        WalV8Header {
            timeline_id: TimelineId([0x11; 16]),
            archive_epoch_id,
            segment_id: SegmentId(7),
            predecessor: crate::pitr::ChainAnchor::Genesis { archive_epoch_id },
            incarnation: [0x44; 16],
        }
    }

    fn install_frame(image: &mut Vec<u8>, frame_offset: u64, frame: &[u8]) -> Result<()> {
        let start = usize::try_from(frame_offset)?;
        let end = start
            .checked_add(frame.len())
            .context("test frame end overflows")?;
        if image.len() < end {
            image.resize(end, 0);
        }
        image[start..end].copy_from_slice(frame);
        Ok(())
    }

    fn empty_image() -> Result<EmptyImage> {
        let header = test_header();
        let header_bytes = header.encode()?;
        let physical_header_digest: [u8; 32] = Sha256::digest(header_bytes).into();
        let initial_prefix_digest = header.initial_prefix_digest()?;
        let generation_zero_frame =
            encode_generation_zero_frontier(physical_header_digest, initial_prefix_digest)?;
        let generation_zero = WalV8Frontier::decode_body(
            &decode_frame_structural(
                &generation_zero_frame,
                HEADER_LEN_U64,
                &physical_header_digest,
                &initial_prefix_digest,
            )?
            .body,
        )?;
        let generation_zero_digest = frame_record_digest(&generation_zero_frame)?;
        let mut image = header_bytes.to_vec();
        install_frame(&mut image, HEADER_LEN_U64, &generation_zero_frame)?;
        Ok((
            image,
            physical_header_digest,
            generation_zero,
            generation_zero_digest,
        ))
    }

    fn append_batch(
        image: &mut Vec<u8>,
        physical_header_digest: [u8; 32],
        frame_offset: u64,
        ticket: u64,
        commit_ts: u64,
    ) -> Result<(u64, WalBatch, WalV8Digest, u64)> {
        let batch = WalBatch {
            commit_ts,
            recorded_at: RecordedAt {
                secs: 100 + i64::try_from(ticket)?,
                nanos: 123,
            },
            entries: vec![WalEntry::Put {
                key: format!("key-{ticket}").into_bytes(),
                value: format!("value-{ticket}").into_bytes(),
            }],
        };
        let encoded_v5 = encode_v5_batch(&batch, LIVE_WAL_V5_LIMITS)?;
        let data_len = usize::try_from(u32::from_be_bytes(
            encoded_v5[24..28]
                .try_into()
                .expect("v5 data length is four bytes"),
        ))?;
        let entries = &encoded_v5
            [crate::pitr::WAL_V5_BATCH_HEADER_LEN..crate::pitr::WAL_V5_BATCH_HEADER_LEN + data_len];
        let logical_header = WalV8LogicalBatchHeader {
            segment_ticket: ticket,
            commit_ts,
            recorded_at_secs: batch.recorded_at.secs,
            recorded_at_nanos: batch.recorded_at.nanos,
            entry_count: u32::try_from(batch.entries.len())?,
        }
        .encode(entries, LIVE_WAL_V5_LIMITS)?;
        let mut logical_bytes = Vec::with_capacity(logical_header.len() + entries.len());
        logical_bytes.extend_from_slice(&logical_header);
        logical_bytes.extend_from_slice(entries);

        let entry_digest = {
            let mut hasher = WalV8EntryHasher::new(u64::try_from(entries.len())?);
            hasher.update(entries)?;
            hasher.finish()?
        };
        let batch_digest = digest::batch_digest(&logical_header, &entry_digest);
        let logical_batch_len = u64::try_from(logical_bytes.len())?;

        let fragment_count = data_fragment_count(u64::try_from(logical_bytes.len())?)
            .context("test batch has no valid v8 fragment count")?;
        let fragment_count_usize = usize::try_from(fragment_count)?;
        for (fragment_index, payload) in logical_bytes
            .chunks(WAL_V8_DATA_FRAGMENT_PAYLOAD_CAPACITY)
            .enumerate()
        {
            let frame_offset = frame_offset
                .checked_add(
                    u64::try_from(fragment_index)?
                        .checked_mul(FRAME_LEN_U64)
                        .context("test fragment offset overflows")?,
                )
                .context("test fragment offset overflows")?;
            let fragment = WalV8DataFragmentHeader {
                segment_ticket: ticket,
                fragment_index: u32::try_from(fragment_index)?,
                fragment_count,
                batch_bytes: u64::try_from(logical_bytes.len())?,
            };
            let body = fragment.encode_body(payload)?;
            let frame = encode_data_frame(
                physical_header_digest,
                test_header().initial_prefix_digest()?,
                frame_offset,
                &body,
            )?;
            install_frame(image, frame_offset, &frame)?;
        }
        let end = frame_offset
            .checked_add(
                u64::try_from(fragment_count_usize)?
                    .checked_mul(FRAME_LEN_U64)
                    .context("test DATA end overflows")?,
            )
            .context("test DATA end overflows")?;
        Ok((end, batch, batch_digest, logical_batch_len))
    }

    struct FrontierAppend {
        previous: WalV8Frontier,
        previous_offset: u64,
        previous_digest: [u8; 32],
        frame_offset: u64,
        ticket_end: u64,
        durable_end: u64,
        last_commit_ts: u64,
        prefix_digest: WalV8Digest,
    }

    fn append_frontier(
        image: &mut Vec<u8>,
        physical_header_digest: [u8; 32],
        append: FrontierAppend,
    ) -> Result<(WalV8Frontier, [u8; 32])> {
        let FrontierAppend {
            previous,
            previous_offset,
            previous_digest,
            frame_offset,
            ticket_end,
            durable_end,
            last_commit_ts,
            prefix_digest,
        } = append;
        let frontier = WalV8Frontier {
            generation: previous.generation + 1,
            ticket_end,
            durable_end,
            last_commit_ts,
            prefix_digest,
            previous_frontier_offset: previous_offset,
            previous_frontier_digest: previous_digest,
        };
        let frame = encode_frontier_successor(
            frontier,
            previous,
            previous_offset,
            &previous_digest,
            frame_offset,
            physical_header_digest,
            test_header().initial_prefix_digest()?,
        )?;
        let frame_digest = frame_record_digest(&frame)?;
        install_frame(image, frame_offset, &frame)?;
        Ok((frontier, frame_digest))
    }

    fn active_boundary(frontier: WalV8Frontier) -> ActiveBoundary {
        let header = test_header();
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

    struct TwoBatchImage {
        bytes: Vec<u8>,
        physical_header_digest: [u8; 32],
        first_frontier: WalV8Frontier,
        first_frontier_digest: [u8; 32],
        last_frontier: WalV8Frontier,
        final_prefix: WalV8Digest,
    }

    fn two_batch_image() -> Result<TwoBatchImage> {
        let (mut bytes, physical_header_digest, generation_zero, generation_zero_digest) =
            empty_image()?;
        let initial_prefix = test_header().initial_prefix_digest()?;
        let (first_data_end, first_batch, first_batch_digest, first_batch_len) =
            append_batch(&mut bytes, physical_header_digest, 8192, 0, 10)?;
        let first_prefix =
            digest::advance_prefix(&initial_prefix, 0, first_batch_len, &first_batch_digest);
        let (first_frontier, first_frontier_digest) = append_frontier(
            &mut bytes,
            physical_header_digest,
            FrontierAppend {
                previous: generation_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: generation_zero_digest,
                frame_offset: first_data_end,
                ticket_end: 1,
                durable_end: first_data_end,
                last_commit_ts: first_batch.commit_ts,
                prefix_digest: first_prefix,
            },
        )?;

        let (second_data_end, second_batch, second_batch_digest, second_batch_len) = append_batch(
            &mut bytes,
            physical_header_digest,
            first_data_end + FRAME_LEN_U64,
            1,
            20,
        )?;
        let final_prefix =
            digest::advance_prefix(&first_prefix, 1, second_batch_len, &second_batch_digest);
        let (last_frontier, _) = append_frontier(
            &mut bytes,
            physical_header_digest,
            FrontierAppend {
                previous: first_frontier,
                previous_offset: first_data_end,
                previous_digest: first_frontier_digest,
                frame_offset: second_data_end,
                ticket_end: 2,
                durable_end: second_data_end,
                last_commit_ts: second_batch.commit_ts,
                prefix_digest: final_prefix,
            },
        )?;

        Ok(TwoBatchImage {
            bytes,
            physical_header_digest,
            first_frontier,
            first_frontier_digest,
            last_frontier,
            final_prefix,
        })
    }

    fn discovery(
        image: &[u8],
        physical_header_digest: &[u8; 32],
    ) -> Result<WalV8FrontierDiscovery> {
        let mut reader = Cursor::new(image);
        discover_frontier_candidates(
            &mut reader,
            u64::try_from(image.len())?,
            physical_header_digest,
            &test_header().initial_prefix_digest()?,
        )
    }

    fn select(
        image: &[u8],
        physical_header_digest: &[u8; 32],
        anchors: &[ActiveBoundary],
    ) -> Result<WalV8RecoverySelection> {
        let mut reader = Cursor::new(image);
        let discovered = discovery(image, physical_header_digest)?;
        select_active_candidate(
            &mut reader,
            u64::try_from(image.len())?,
            &discovered,
            anchors,
            test_header().predecessor,
            LIVE_WAL_V5_LIMITS,
        )
    }

    struct FailingReader {
        cursor: Cursor<Vec<u8>>,
        fail_at: u64,
    }

    impl Read for FailingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.cursor.position() == self.fail_at {
                return Err(io::Error::other("injected recovery read failure"));
            }
            self.cursor.read(buffer)
        }
    }

    impl Seek for FailingReader {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            self.cursor.seek(position)
        }
    }

    #[test]
    fn active_recovery_selects_latest_frontier_and_checks_intermediate_anchor() -> Result<()> {
        let image = two_batch_image()?;
        let anchor = active_boundary(image.first_frontier);
        let selected = select(&image.bytes, &image.physical_header_digest, &[anchor])?;

        assert_eq!(selected.candidate.frontier, image.last_frontier);
        assert_eq!(
            selected.candidate.frontier.prefix_digest,
            image.final_prefix
        );
        assert_eq!(selected.batches.len(), 2);
        assert_eq!(selected.batches[0].commit_ts, 10);
        assert_eq!(selected.batches[1].commit_ts, 20);
        assert_eq!(selected.last_frontier, image.first_frontier);
        assert_eq!(
            selected.last_frontier_offset,
            image.first_frontier.durable_end
        );
        assert_eq!(selected.last_frontier_digest, image.first_frontier_digest);

        let mut bad_anchor = anchor;
        bad_anchor.prefix_digest[0] ^= 1;
        assert!(select(&image.bytes, &image.physical_header_digest, &[bad_anchor],).is_err());
        Ok(())
    }

    #[test]
    fn active_recovery_falls_back_to_older_candidate_above_hard_anchor() -> Result<()> {
        let mut image = two_batch_image()?;
        let data_offset = usize::try_from(image.first_frontier.durable_end + FRAME_LEN_U64)?;
        image.bytes[data_offset + 64] ^= 0x80;
        let anchor = active_boundary(image.first_frontier);
        let selected = select(&image.bytes, &image.physical_header_digest, &[anchor])?;

        assert_eq!(selected.candidate.frontier, image.first_frontier);
        assert_eq!(selected.batches.len(), 1);
        assert_eq!(selected.batches[0].commit_ts, 10);
        Ok(())
    }

    #[test]
    fn active_recovery_does_not_fallback_after_a_prefix_read_error() -> Result<()> {
        let image = two_batch_image()?;
        let discovered = discovery(&image.bytes, &image.physical_header_digest)?;
        let mut reader = FailingReader {
            cursor: Cursor::new(image.bytes.clone()),
            fail_at: image.first_frontier.durable_end + FRAME_LEN_U64,
        };
        let error = match select_active_candidate(
            &mut reader,
            u64::try_from(image.bytes.len())?,
            &discovered,
            &[active_boundary(image.first_frontier)],
            test_header().predecessor,
            LIVE_WAL_V5_LIMITS,
        ) {
            Err(error) => error,
            Ok(_) => anyhow::bail!("an I/O failure in the candidate prefix was ignored"),
        };

        assert!(format!("{error:#}").contains("injected recovery read failure"));
        Ok(())
    }

    #[test]
    fn active_recovery_normalizes_fallback_into_a_fresh_inode() -> Result<()> {
        let fixture = two_batch_image()?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let mut damaged = fixture.bytes.clone();
        let second_data_offset =
            usize::try_from(fixture.first_frontier.durable_end + FRAME_LEN_U64)?;
        damaged[second_data_offset + 64] ^= 0x80;
        std::fs::write(&path, damaged)?;
        let original = std::fs::metadata(&path)?;
        let anchor = active_boundary(fixture.first_frontier);
        let header = test_header();
        let installed = crate::wal::v8::install::recover_and_install_active(
            &path,
            &[anchor],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            crate::wal::MAX_WAL_FILE_SIZE,
        )?;

        let installed_metadata = std::fs::metadata(&path)?;
        assert_ne!(original.ino(), installed_metadata.ino());
        assert_eq!(installed.next_ticket, 1);
        assert_eq!(installed.frontier.ticket_end, 1);
        assert_eq!(
            installed.image_len,
            fixture.first_frontier.durable_end + FRAME_LEN_U64
        );
        assert_eq!(installed.selection.batches.len(), 1);
        assert_eq!(installed_metadata.len(), installed.image_len);

        let mut installed_file = File::open(&path)?;
        let initial_prefix_digest = header.initial_prefix_digest()?;
        let discovered = discover_frontier_candidates(
            &mut installed_file,
            installed.image_len,
            &installed.header_digest,
            &initial_prefix_digest,
        )?;
        let reopened = select_active_candidate(
            &mut installed_file,
            installed.image_len,
            &discovered,
            &installed.selection.hard_anchors,
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
        )?;
        assert_eq!(reopened.active_boundary, installed.active_boundary);
        assert_eq!(reopened.candidate.frontier, installed.frontier);
        assert_eq!(reopened.batches.len(), 1);
        Ok(())
    }

    #[test]
    fn active_recovery_rejects_an_intermediate_frontier_fork() -> Result<()> {
        let (mut bytes, physical_header_digest, generation_zero, generation_zero_digest) =
            empty_image()?;
        let initial_prefix = test_header().initial_prefix_digest()?;
        let (first_data_end, first_batch, first_batch_digest, first_batch_len) =
            append_batch(&mut bytes, physical_header_digest, 8192, 0, 10)?;
        let second_data_start = first_data_end + FRAME_LEN_U64;
        let first_prefix =
            digest::advance_prefix(&initial_prefix, 0, first_batch_len, &first_batch_digest);
        let (first_frontier, _first_digest) = append_frontier(
            &mut bytes,
            physical_header_digest,
            FrontierAppend {
                previous: generation_zero,
                previous_offset: HEADER_LEN_U64,
                previous_digest: generation_zero_digest,
                frame_offset: first_data_end,
                ticket_end: 1,
                durable_end: first_data_end,
                last_commit_ts: first_batch.commit_ts,
                prefix_digest: first_prefix,
            },
        )?;
        let (second_data_end, second_batch, second_batch_digest, second_batch_len) =
            append_batch(&mut bytes, physical_header_digest, second_data_start, 1, 20)?;
        let sibling_offset = second_data_end;
        let sibling = WalV8Frontier {
            generation: 1,
            ticket_end: 2,
            durable_end: second_data_end,
            last_commit_ts: 20,
            prefix_digest: digest::advance_prefix(
                &first_prefix,
                1,
                second_batch_len,
                &second_batch_digest,
            ),
            previous_frontier_offset: HEADER_LEN_U64,
            previous_frontier_digest: generation_zero_digest,
        };
        let sibling_frame = encode_frontier_successor(
            sibling,
            generation_zero,
            HEADER_LEN_U64,
            &generation_zero_digest,
            sibling_offset,
            physical_header_digest,
            initial_prefix,
        )?;
        install_frame(&mut bytes, sibling_offset, &sibling_frame)?;

        let anchor = ActiveBoundary {
            ticket_end: 2,
            durable_end: second_data_end,
            last_commit_ts: second_batch.commit_ts,
            prefix_digest: sibling.prefix_digest,
            ..active_boundary(first_frontier)
        };
        assert!(select(&bytes, &physical_header_digest, &[anchor]).is_err());
        Ok(())
    }
}
