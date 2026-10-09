//! Deterministic persistence model for the v7 codec and one-sync protocol.
//!
//! This is a test oracle, not a production recovery implementation. Its small
//! scanner deliberately supports single-writer batches while exercising the
//! on-disk codec, stable/cached persistence states, and the RFC's certificate
//! boundaries.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

use super::codec::{
    WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY, WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN,
    WAL_V7_LOGICAL_BATCH_HEADER_LEN, WalV7DataFragmentHeader, WalV7FrameKind, WalV7Frontier,
    WalV7Header, WalV7LogicalBatchHeader, decode_frame_structural, decode_frontier_successor,
    encode_data_frame, encode_frontier_successor, encode_generation_zero_frontier,
    frame_record_digest,
};
use crate::pitr::{
    ArchiveEpochId, ChainAnchor, LIVE_WAL_V5_LIMITS, RecordedAt, SegmentId, TimelineId, WalBatch,
    WalEntry,
};

const ACTIVE_WAL_PATH: &str = "active.wal";
const TEMP_WAL_PATH: &str = "active.wal.tmp";
const FIRST_DATA_OFFSET: u64 = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveAnchor {
    segment_id: SegmentId,
    incarnation: [u8; 16],
    ticket_end: u64,
    durable_end: u64,
    last_commit_ts: u64,
    prefix_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ImageView {
    Cached,
    Stable,
}

#[derive(Debug, Default)]
struct ModelFile {
    cached: Vec<u8>,
    stable: Vec<u8>,
    sync_attempts: u64,
    successful_syncs: u64,
}

/// A deterministic model of file data, directory entries, manifest state, and
/// the independent client acknowledgement oracle.
#[derive(Debug, Default)]
struct PersistenceModel {
    files: BTreeMap<u64, ModelFile>,
    cached_paths: BTreeMap<String, u64>,
    stable_paths: BTreeMap<String, u64>,
    next_inode: u64,
    cached_active_anchor: Option<ActiveAnchor>,
    stable_active_anchor: Option<ActiveAnchor>,
    directory_sync_attempts: u64,
    manifest_sync_attempts: u64,
    process_restarts: u64,
    process_generation: u64,
}

impl PersistenceModel {
    fn create_file(&mut self, path: &str) -> Result<u64> {
        ensure!(
            !self.cached_paths.contains_key(path),
            "model path already exists"
        );
        let inode = self.next_inode;
        self.next_inode = self
            .next_inode
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("model inode counter overflows"))?;
        self.files.insert(inode, ModelFile::default());
        self.cached_paths.insert(path.to_owned(), inode);
        Ok(inode)
    }

    fn write_at(&mut self, inode: u64, offset: usize, bytes: &[u8]) -> Result<()> {
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| anyhow::anyhow!("model write range overflows"))?;
        let file = self
            .files
            .get_mut(&inode)
            .ok_or_else(|| anyhow::anyhow!("model inode does not exist"))?;
        if file.cached.len() < end {
            file.cached.resize(end, 0);
        }
        file.cached[offset..end].copy_from_slice(bytes);
        Ok(())
    }

    fn fdatasync_success(&mut self, inode: u64) -> Result<()> {
        let file = self
            .files
            .get_mut(&inode)
            .ok_or_else(|| anyhow::anyhow!("model inode does not exist"))?;
        file.sync_attempts += 1;
        file.stable.clone_from(&file.cached);
        file.successful_syncs += 1;
        Ok(())
    }

    /// Applies only the listed cached ranges to stable storage, modeling an
    /// interrupted sync whose persistence order is not known to the caller.
    fn fdatasync_interrupted(
        &mut self,
        inode: u64,
        ranges: &[std::ops::Range<usize>],
    ) -> Result<()> {
        let file = self
            .files
            .get_mut(&inode)
            .ok_or_else(|| anyhow::anyhow!("model inode does not exist"))?;
        for range in ranges {
            ensure!(
                range.start <= range.end && range.end <= file.cached.len(),
                "model persistence range is outside cached file"
            );
        }
        file.sync_attempts += 1;
        for range in ranges {
            if file.stable.len() < range.end {
                file.stable.resize(range.end, 0);
            }
            file.stable[range.clone()].copy_from_slice(&file.cached[range.clone()]);
        }
        Ok(())
    }

    fn sync_directory_success(&mut self) {
        self.directory_sync_attempts += 1;
        self.stable_paths.clone_from(&self.cached_paths);
    }

    fn publish_active_anchor(&mut self, anchor: ActiveAnchor) {
        self.cached_active_anchor = Some(anchor);
    }

    fn sync_manifest_success(&mut self) {
        self.manifest_sync_attempts += 1;
        self.stable_active_anchor = self.cached_active_anchor;
    }

    /// A process restart preserves the kernel's cached file and directory view.
    fn restart_process_without_reboot(&mut self) {
        self.process_restarts += 1;
        self.process_generation += 1;
    }

    /// A power loss discards every cached change that did not reach stable state.
    fn power_loss(&mut self) {
        self.process_generation += 1;
        for file in self.files.values_mut() {
            file.cached.clone_from(&file.stable);
        }
        self.cached_paths.clone_from(&self.stable_paths);
        self.cached_active_anchor = self.stable_active_anchor;
    }

    fn rename_replace(&mut self, source: &str, target: &str) -> Result<Option<u64>> {
        let inode = self
            .cached_paths
            .remove(source)
            .ok_or_else(|| anyhow::anyhow!("model rename source does not exist"))?;
        Ok(self.cached_paths.insert(target.to_owned(), inode))
    }

    fn cleanup_unreferenced_inodes(&mut self) -> usize {
        let referenced: BTreeSet<u64> = self
            .cached_paths
            .values()
            .chain(self.stable_paths.values())
            .copied()
            .collect();
        let before = self.files.len();
        self.files.retain(|inode, _| referenced.contains(inode));
        before - self.files.len()
    }

    fn cleanup_uninstalled_active_file(&mut self) -> Result<usize> {
        ensure!(
            self.stable_active_anchor.is_none(),
            "cannot orphan-clean an Active WAL with a durable manifest anchor"
        );
        // Unlink changes only the cached directory. A stable directory entry
        // continues to pin the inode until a later successful directory sync.
        self.cached_paths.remove(ACTIVE_WAL_PATH);
        Ok(self.cleanup_unreferenced_inodes())
    }

    fn path_inode(&self, path: &str, view: ImageView) -> Option<u64> {
        match view {
            ImageView::Cached => self.cached_paths.get(path),
            ImageView::Stable => self.stable_paths.get(path),
        }
        .copied()
    }

    fn canonical_inode_matches(&self, path: &str, inode: u64) -> bool {
        self.path_inode(path, ImageView::Cached) == Some(inode)
            && self.path_inode(path, ImageView::Stable) == Some(inode)
    }

    fn path_bytes(&self, path: &str, view: ImageView) -> Option<&[u8]> {
        let inode = self.path_inode(path, view)?;
        let file = self.files.get(&inode)?;
        Some(match view {
            ImageView::Cached => &file.cached,
            ImageView::Stable => &file.stable,
        })
    }

    fn file(&self, inode: u64) -> Result<&ModelFile> {
        self.files
            .get(&inode)
            .ok_or_else(|| anyhow::anyhow!("model inode does not exist"))
    }

    fn stable_active_anchor(&self) -> Option<ActiveAnchor> {
        self.stable_active_anchor
    }

    fn service_ready_for(&self, path: &str, inode: u64) -> bool {
        if !self.canonical_inode_matches(path, inode) {
            return false;
        }
        // Unsynced cached bytes can expose a non-durable candidate after a
        // process restart; service requires the active image to be fully synced.
        if self.path_bytes(path, ImageView::Cached) != self.path_bytes(path, ImageView::Stable) {
            return false;
        }
        let (Some(anchor), Some(bytes)) = (
            self.stable_active_anchor,
            self.path_bytes(path, ImageView::Stable),
        ) else {
            return false;
        };
        let Ok(header) = WalV7Header::decode(bytes) else {
            return false;
        };
        if header.segment_id != anchor.segment_id || header.incarnation != anchor.incarnation {
            return false;
        }
        let Ok(recovered) = recover_latest(bytes) else {
            return false;
        };
        if verify_active_anchor(bytes, anchor).is_err() {
            return false;
        }
        recovered.frontier.ticket_end >= anchor.ticket_end
            && recovered.frontier.durable_end >= anchor.durable_end
            && recovered.frontier.last_commit_ts >= anchor.last_commit_ts
    }
}

#[derive(Debug)]
struct PendingCommit {
    ticket: u64,
    commit_ts: u64,
    recorded_at: RecordedAt,
    data_offsets: Vec<u64>,
    frontier_offset: u64,
    frontier_frame: [u8; WAL_V7_FRAME_LEN],
    frontier: WalV7Frontier,
    next_logical_hash: Sha256,
}

#[derive(Debug, Default)]
struct WriteHistory {
    published_tickets: Vec<u64>,
    acknowledged_tickets: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitPhase {
    Idle,
    AwaitingSync { ticket: u64 },
    Synced { ticket: u64 },
    Poisoned,
}

#[derive(Debug)]
struct SerialWriter {
    inode: u64,
    process_generation: u64,
    header_digest: [u8; 32],
    logical_hash: Sha256,
    previous_frontier: WalV7Frontier,
    previous_frontier_offset: u64,
    previous_frontier_digest: [u8; 32],
    next_ticket: u64,
    next_data_offset: u64,
    last_commit_ts: u64,
    last_recorded_at: Option<RecordedAt>,
    phase: CommitPhase,
}

#[derive(Debug)]
struct SerialHarness {
    disk: PersistenceModel,
    writer: SerialWriter,
    history: WriteHistory,
}

impl SerialHarness {
    fn ensure_live_writer(&mut self) -> Result<()> {
        if self.writer.process_generation != self.disk.process_generation {
            self.writer.phase = CommitPhase::Poisoned;
            anyhow::bail!("v7 serial writer belongs to an earlier process generation");
        }
        if !self
            .disk
            .canonical_inode_matches(ACTIVE_WAL_PATH, self.writer.inode)
        {
            self.writer.phase = CommitPhase::Poisoned;
            anyhow::bail!("v7 serial writer no longer owns the canonical Active WAL");
        }
        Ok(())
    }

    fn create() -> Result<Self> {
        let header = model_header();
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
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

        let mut disk = PersistenceModel::default();
        let inode = disk.create_file(ACTIVE_WAL_PATH)?;
        disk.write_at(inode, 0, &header_bytes)?;
        disk.write_at(inode, WAL_V7_HEADER_LEN, &generation_zero_frame)?;
        disk.fdatasync_success(inode)?;
        disk.sync_directory_success();
        disk.publish_active_anchor(ActiveAnchor {
            segment_id: header.segment_id,
            incarnation: header.incarnation,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
        });
        disk.sync_manifest_success();

        let mut logical_hash = Sha256::new();
        logical_hash.update(header_bytes);
        let process_generation = disk.process_generation;
        Ok(Self {
            disk,
            writer: SerialWriter {
                inode,
                process_generation,
                header_digest,
                logical_hash,
                previous_frontier: generation_zero,
                previous_frontier_offset: WAL_V7_HEADER_LEN as u64,
                previous_frontier_digest: generation_zero_digest,
                next_ticket: 0,
                next_data_offset: FIRST_DATA_OFFSET,
                last_commit_ts: 0,
                last_recorded_at: None,
                phase: CommitPhase::Idle,
            },
            history: WriteHistory::default(),
        })
    }

    fn commit(&mut self, data: &[u8], commit_ts: u64) -> Result<()> {
        let pending = self.begin_commit(data, commit_ts)?;
        self.finish_commit(pending)
    }

    fn begin_commit(&mut self, data: &[u8], commit_ts: u64) -> Result<PendingCommit> {
        self.begin_commit_at(
            data,
            commit_ts,
            RecordedAt {
                secs: -123,
                nanos: 456,
            },
        )
    }

    fn begin_commit_at(
        &mut self,
        data: &[u8],
        commit_ts: u64,
        recorded_at: RecordedAt,
    ) -> Result<PendingCommit> {
        ensure!(
            self.writer.phase == CommitPhase::Idle,
            "v7 serial writer cannot begin in phase {:?}",
            self.writer.phase
        );
        self.ensure_live_writer()?;
        ensure!(
            commit_ts > self.writer.last_commit_ts,
            "commit timestamp did not advance"
        );
        let mut recorded_at_watermark = self.writer.last_recorded_at;
        validate_recorded_time_order(&mut recorded_at_watermark, recorded_at)?;

        let ticket = self.writer.next_ticket;
        let entry_stream = encode_model_entry_stream(data, commit_ts)?;
        let batch_header = WalV7LogicalBatchHeader {
            segment_ticket: ticket,
            commit_ts,
            recorded_at_secs: recorded_at.secs,
            recorded_at_nanos: recorded_at.nanos,
            entry_count: 1,
        }
        .encode(&entry_stream, LIVE_WAL_V5_LIMITS)?;
        let batch_bytes_len = WAL_V7_LOGICAL_BATCH_HEADER_LEN
            .checked_add(entry_stream.len())
            .ok_or_else(|| anyhow::anyhow!("model logical batch length overflows"))?;
        let batch_bytes_len_u64 = u64::try_from(batch_bytes_len)?;
        let mut logical_batch = Vec::with_capacity(batch_bytes_len);
        logical_batch.extend_from_slice(&batch_header);
        logical_batch.extend_from_slice(&entry_stream);

        let fragment_count = batch_bytes_len
            .checked_add(WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY - 1)
            .ok_or_else(|| anyhow::anyhow!("model fragment count overflows"))?
            / WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY;
        let fragment_count = u32::try_from(fragment_count)?;
        let mut data_offsets = Vec::with_capacity(fragment_count as usize);
        // Any failure after writing starts leaves the runtime poisoned. Only
        // completing DATA and FRONTIER exposes a cycle awaiting sync.
        self.writer.phase = CommitPhase::Poisoned;
        for (fragment_index, payload) in logical_batch
            .chunks(WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY)
            .enumerate()
        {
            let frame_delta = u64::try_from(fragment_index)?
                .checked_mul(WAL_V7_FRAME_LEN as u64)
                .ok_or_else(|| anyhow::anyhow!("model DATA offset overflows"))?;
            let frame_offset = self
                .writer
                .next_data_offset
                .checked_add(frame_delta)
                .ok_or_else(|| anyhow::anyhow!("model DATA offset overflows"))?;
            let fragment_header = WalV7DataFragmentHeader {
                segment_ticket: ticket,
                fragment_index: u32::try_from(fragment_index)?,
                fragment_count,
                batch_bytes: batch_bytes_len_u64,
            };
            let body = fragment_header.encode_body(payload)?;
            let frame = encode_data_frame(self.writer.header_digest, frame_offset, &body)?;
            self.disk
                .write_at(self.writer.inode, usize::try_from(frame_offset)?, &frame)?;
            data_offsets.push(frame_offset);
        }

        let data_end = self
            .writer
            .next_data_offset
            .checked_add(
                u64::from(fragment_count)
                    .checked_mul(WAL_V7_FRAME_LEN as u64)
                    .ok_or_else(|| anyhow::anyhow!("model DATA end overflows"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("model DATA end overflows"))?;
        let frontier_offset = data_end;
        let previous_frontier_end = self
            .writer
            .previous_frontier_offset
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("model previous FRONTIER end overflows"))?;
        let mut next_logical_hash = self.writer.logical_hash.clone();
        next_logical_hash.update(&logical_batch);
        let frontier = WalV7Frontier {
            generation: self
                .writer
                .previous_frontier
                .generation
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("model FRONTIER generation overflows"))?,
            ticket_end: ticket
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("model ticket end overflows"))?,
            durable_end: data_end.max(previous_frontier_end),
            last_commit_ts: commit_ts,
            prefix_digest: next_logical_hash.clone().finalize().into(),
            previous_frontier_offset: self.writer.previous_frontier_offset,
            previous_frontier_digest: self.writer.previous_frontier_digest,
        };
        let frontier_frame = encode_frontier_successor(
            frontier,
            self.writer.previous_frontier,
            self.writer.previous_frontier_offset,
            &self.writer.previous_frontier_digest,
            frontier_offset,
            self.writer.header_digest,
        )?;
        self.disk.write_at(
            self.writer.inode,
            usize::try_from(frontier_offset)?,
            &frontier_frame,
        )?;
        self.writer.phase = CommitPhase::AwaitingSync { ticket };

        Ok(PendingCommit {
            ticket,
            commit_ts,
            recorded_at,
            data_offsets,
            frontier_offset,
            frontier_frame,
            frontier,
            next_logical_hash,
        })
    }

    fn finish_commit(&mut self, pending: PendingCommit) -> Result<()> {
        self.sync_pending_commit(&pending)?;
        self.publish_commit(pending)
    }

    fn sync_pending_commit(&mut self, pending: &PendingCommit) -> Result<()> {
        ensure!(
            self.writer.phase
                == (CommitPhase::AwaitingSync {
                    ticket: pending.ticket,
                }),
            "v7 serial writer is not awaiting this commit's sync"
        );
        self.ensure_live_writer()?;
        self.writer.phase = CommitPhase::Poisoned;
        self.disk.fdatasync_success(self.writer.inode)?;
        self.writer.phase = CommitPhase::Synced {
            ticket: pending.ticket,
        };
        Ok(())
    }

    fn interrupt_sync(&mut self, persisted_ranges: &[std::ops::Range<usize>]) -> Result<()> {
        ensure!(
            matches!(self.writer.phase, CommitPhase::AwaitingSync { .. }),
            "v7 serial writer has no commit awaiting sync"
        );
        self.ensure_live_writer()?;
        self.writer.phase = CommitPhase::Poisoned;
        self.disk
            .fdatasync_interrupted(self.writer.inode, persisted_ranges)?;
        Ok(())
    }

    fn publish_commit(&mut self, pending: PendingCommit) -> Result<()> {
        ensure!(
            self.writer.phase
                == (CommitPhase::Synced {
                    ticket: pending.ticket,
                }),
            "v7 serial writer cannot publish a commit without its successful sync"
        );
        self.ensure_live_writer()?;
        self.writer.phase = CommitPhase::Poisoned;
        let frontier_digest = frame_record_digest(&pending.frontier_frame)?;
        let next_data_offset = pending
            .frontier_offset
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("model next DATA offset overflows"))?;
        self.writer.logical_hash = pending.next_logical_hash;
        self.writer.previous_frontier = pending.frontier;
        self.writer.previous_frontier_offset = pending.frontier_offset;
        self.writer.previous_frontier_digest = frontier_digest;
        self.writer.next_ticket = pending.frontier.ticket_end;
        self.writer.next_data_offset = next_data_offset;
        self.writer.last_commit_ts = pending.commit_ts;
        self.writer.last_recorded_at = Some(pending.recorded_at);
        self.history.published_tickets.push(pending.ticket);
        self.history.acknowledged_tickets.push(pending.ticket);
        self.writer.phase = CommitPhase::Idle;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedBoundary {
    frame_offset: u64,
    frontier: WalV7Frontier,
    batches: Vec<WalBatch>,
}

/// Finds the newest marker whose complete prefix and immediate chain verify.
/// This simple, one-writer oracle is intentionally separate from production
/// recovery, which is implemented in the next plan stage.
fn recover_latest(image: &[u8]) -> Result<VerifiedBoundary> {
    ensure!(
        image.len() >= WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN,
        "missing v7 generation-zero frame"
    );
    let header = WalV7Header::decode(image)?;
    let header_digest = header.digest()?;
    let generation_zero_offset = WAL_V7_HEADER_LEN as u64;
    let generation_zero_frame = frame_at(image, generation_zero_offset)?;
    let generation_zero_decoded = decode_frame_structural(
        generation_zero_frame,
        generation_zero_offset,
        &header_digest,
    )?;
    ensure!(
        generation_zero_decoded.kind == WalV7FrameKind::Frontier,
        "model WAL generation-zero slot is not a FRONTIER"
    );
    let generation_zero = WalV7Frontier::decode_body(&generation_zero_decoded.body)?;
    generation_zero.validate_generation_zero(&header_digest, generation_zero_offset)?;
    let generation_zero_digest = frame_record_digest(generation_zero_frame)?;
    let fallback = VerifiedBoundary {
        frame_offset: generation_zero_offset,
        frontier: generation_zero,
        batches: Vec::new(),
    };

    let last_frame_offset = image
        .len()
        .checked_sub(WAL_V7_FRAME_LEN)
        .map(|offset| offset / WAL_V7_FRAME_LEN * WAL_V7_FRAME_LEN);
    let Some(mut candidate_offset) = last_frame_offset else {
        return Ok(fallback);
    };
    while candidate_offset > WAL_V7_HEADER_LEN {
        let candidate_offset_u64 = u64::try_from(candidate_offset)?;
        if let Ok(frame) = frame_at(image, candidate_offset_u64)
            && let Ok(decoded) =
                decode_frame_structural(frame, candidate_offset_u64, &header_digest)
            && decoded.kind == WalV7FrameKind::Frontier
            && let Ok(verified) = verify_candidate(
                image,
                &header,
                &header_digest,
                generation_zero,
                generation_zero_offset,
                &generation_zero_digest,
                candidate_offset_u64,
            )
        {
            return Ok(verified);
        }
        candidate_offset -= WAL_V7_FRAME_LEN;
    }
    Ok(fallback)
}

fn verify_candidate(
    image: &[u8],
    header: &WalV7Header,
    header_digest: &[u8; 32],
    generation_zero: WalV7Frontier,
    generation_zero_offset: u64,
    generation_zero_digest: &[u8; 32],
    candidate_offset: u64,
) -> Result<VerifiedBoundary> {
    let mut logical_hash = Sha256::new();
    logical_hash.update(header.encode()?);
    let mut expected_ticket = 0_u64;
    let mut last_commit_ts = 0_u64;
    let mut last_recorded_at = None;
    let mut last_data_end = WAL_V7_HEADER_LEN as u64;
    let mut previous_frontier = generation_zero;
    let mut previous_frontier_offset = generation_zero_offset;
    let mut previous_frontier_digest = *generation_zero_digest;
    let candidate_frame = frame_at(image, candidate_offset)?;
    let candidate_decoded =
        decode_frame_structural(candidate_frame, candidate_offset, header_digest)?;
    ensure!(
        candidate_decoded.kind == WalV7FrameKind::Frontier,
        "model candidate is not a FRONTIER"
    );
    let candidate_body = WalV7Frontier::decode_body(&candidate_decoded.body)?;
    ensure!(
        candidate_body.durable_end <= candidate_offset,
        "model candidate extends beyond its physical offset"
    );

    let mut batches = Vec::new();
    let mut physical_offset = FIRST_DATA_OFFSET;
    while physical_offset < candidate_body.durable_end {
        let frame = frame_at(image, physical_offset)?;
        let decoded = decode_frame_structural(frame, physical_offset, header_digest)?;
        match decoded.kind {
            WalV7FrameKind::Data => {
                let (logical_header, batch_bytes, batch, next_offset) = decode_model_batch(
                    image,
                    physical_offset,
                    candidate_body.durable_end,
                    expected_ticket,
                    last_commit_ts,
                    header_digest,
                )?;
                validate_recorded_time_order(&mut last_recorded_at, batch.recorded_at)?;
                logical_hash.update(&batch_bytes);
                batches.push(batch);
                expected_ticket = expected_ticket
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("model ticket count overflows"))?;
                last_commit_ts = logical_header.commit_ts;
                last_data_end = next_offset;
                physical_offset = next_offset;
            }
            WalV7FrameKind::Frontier => {
                let (frontier, record_digest) = decode_frontier_successor(
                    frame,
                    physical_offset,
                    header_digest,
                    previous_frontier,
                    previous_frontier_offset,
                    &previous_frontier_digest,
                )?;
                verify_boundary_contents(
                    frontier,
                    expected_ticket,
                    last_commit_ts,
                    last_data_end,
                    previous_frontier_offset,
                    logical_hash.clone().finalize().into(),
                )?;
                previous_frontier = frontier;
                previous_frontier_offset = physical_offset;
                previous_frontier_digest = record_digest;
                physical_offset += WAL_V7_FRAME_LEN as u64;
            }
        }
    }
    ensure!(
        physical_offset == candidate_body.durable_end,
        "model covered prefix ends inside a frame or batch"
    );

    let (frontier, _) = decode_frontier_successor(
        candidate_frame,
        candidate_offset,
        header_digest,
        previous_frontier,
        previous_frontier_offset,
        &previous_frontier_digest,
    )?;
    verify_boundary_contents(
        frontier,
        expected_ticket,
        last_commit_ts,
        last_data_end,
        previous_frontier_offset,
        logical_hash.finalize().into(),
    )?;
    Ok(VerifiedBoundary {
        frame_offset: candidate_offset,
        frontier,
        batches,
    })
}

fn decode_model_batch(
    image: &[u8],
    first_offset: u64,
    prefix_end: u64,
    expected_ticket: u64,
    last_commit_ts: u64,
    header_digest: &[u8; 32],
) -> Result<(WalV7LogicalBatchHeader, Vec<u8>, WalBatch, u64)> {
    let first_frame = frame_at(image, first_offset)?;
    let first_decoded = decode_frame_structural(first_frame, first_offset, header_digest)?;
    ensure!(
        first_decoded.kind == WalV7FrameKind::Data,
        "model batch starts with a control frame"
    );
    let (first_fragment, first_payload) =
        WalV7DataFragmentHeader::decode_body(&first_decoded.body)?;
    ensure!(
        first_fragment.segment_ticket == expected_ticket && first_fragment.fragment_index == 0,
        "model DATA ticket or first fragment index is invalid"
    );
    let fragment_count = usize::try_from(first_fragment.fragment_count)?;
    let batch_len = usize::try_from(first_fragment.batch_bytes)?;
    ensure!(fragment_count > 0, "model DATA batch has no fragments");
    let max_batch_len = WAL_V7_LOGICAL_BATCH_HEADER_LEN
        .checked_add(LIVE_WAL_V5_LIMITS.max_batch_data_bytes)
        .ok_or_else(|| anyhow::anyhow!("model batch byte limit overflows"))?;
    ensure!(
        batch_len <= max_batch_len,
        "model DATA batch exceeds configured byte limit"
    );
    let next_offset = first_offset
        .checked_add(
            u64::from(first_fragment.fragment_count)
                .checked_mul(WAL_V7_FRAME_LEN as u64)
                .ok_or_else(|| anyhow::anyhow!("model batch span overflows"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("model batch end overflows"))?;
    ensure!(
        next_offset <= prefix_end && next_offset <= u64::try_from(image.len())?,
        "model DATA batch extends beyond the available covered prefix"
    );
    let mut batch_bytes = Vec::new();
    batch_bytes.try_reserve_exact(batch_len)?;
    batch_bytes.extend_from_slice(first_payload);
    for fragment_index in 1..fragment_count {
        let delta = u64::try_from(fragment_index)?
            .checked_mul(WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("model fragment offset overflows"))?;
        let offset = first_offset
            .checked_add(delta)
            .ok_or_else(|| anyhow::anyhow!("model fragment offset overflows"))?;
        let decoded = decode_frame_structural(frame_at(image, offset)?, offset, header_digest)?;
        ensure!(
            decoded.kind == WalV7FrameKind::Data,
            "model DATA batch is interleaved"
        );
        let (fragment, payload) = WalV7DataFragmentHeader::decode_body(&decoded.body)?;
        ensure!(
            fragment.segment_ticket == first_fragment.segment_ticket
                && fragment.fragment_index == u32::try_from(fragment_index)?
                && fragment.fragment_count == first_fragment.fragment_count
                && fragment.batch_bytes == first_fragment.batch_bytes,
            "model DATA fragments disagree"
        );
        batch_bytes.extend_from_slice(payload);
    }
    ensure!(
        batch_bytes.len() == batch_len,
        "model DATA fragments have the wrong total length"
    );
    ensure!(
        batch_bytes.len() >= WAL_V7_LOGICAL_BATCH_HEADER_LEN,
        "model batch header is truncated"
    );
    let data = &batch_bytes[WAL_V7_LOGICAL_BATCH_HEADER_LEN..];
    let logical_header = WalV7LogicalBatchHeader::decode(
        &batch_bytes[..WAL_V7_LOGICAL_BATCH_HEADER_LEN],
        data,
        expected_ticket,
        first_fragment.batch_bytes,
        LIVE_WAL_V5_LIMITS,
    )?;
    ensure!(
        logical_header.commit_ts > last_commit_ts,
        "model commit timestamps are not increasing"
    );
    let decoded_batch = decode_model_entry_stream(logical_header, data)?;
    Ok((logical_header, batch_bytes, decoded_batch, next_offset))
}

/// Uses the production v5-family batch encoder to make a valid RFC 023 entry
/// stream for the v7 logical-batch payload.
fn encode_model_entry_stream(value: &[u8], commit_ts: u64) -> Result<Vec<u8>> {
    let batch = WalBatch {
        commit_ts,
        recorded_at: RecordedAt {
            secs: -123,
            nanos: 456,
        },
        entries: vec![WalEntry::Put {
            key: b"model-key".to_vec(),
            value: value.to_vec(),
        }],
    };
    let encoded = crate::pitr::encode_v5_batch(&batch, LIVE_WAL_V5_LIMITS)?;
    let data_len = usize::try_from(u32::from_be_bytes(
        encoded[24..28]
            .try_into()
            .expect("v5 batch data length is four bytes"),
    ))?;
    let data_start = crate::pitr::WAL_V5_BATCH_HEADER_LEN;
    let data_end = data_start
        .checked_add(data_len)
        .ok_or_else(|| anyhow::anyhow!("model v5 entry stream range overflows"))?;
    ensure!(
        data_end <= encoded.len(),
        "model v5 entry stream is truncated"
    );
    Ok(encoded[data_start..data_end].to_vec())
}

/// Re-wraps a v7 logical entry stream in a temporary v5 envelope so the
/// production RFC 023 decoder validates kinds, lengths, flags, and exact use of
/// the declared entry count before the model accepts it.
fn decode_model_entry_stream(header: WalV7LogicalBatchHeader, data: &[u8]) -> Result<WalBatch> {
    let header_len = crate::pitr::WAL_V5_BATCH_HEADER_LEN;
    let data_end = header_len
        .checked_add(data.len())
        .ok_or_else(|| anyhow::anyhow!("model v5 batch length overflows"))?;
    let alignment = crate::pitr::WAL_V5_ALIGNMENT;
    let aligned_len = data_end
        .checked_add(alignment - 1)
        .ok_or_else(|| anyhow::anyhow!("model v5 batch alignment overflows"))?
        / alignment
        * alignment;
    let mut encoded = vec![0; aligned_len];
    encoded[0..8].copy_from_slice(&header.commit_ts.to_be_bytes());
    encoded[8..16].copy_from_slice(&header.recorded_at_secs.to_be_bytes());
    encoded[16..20].copy_from_slice(&header.recorded_at_nanos.to_be_bytes());
    encoded[20..24].copy_from_slice(&header.entry_count.to_be_bytes());
    encoded[24..28].copy_from_slice(&u32::try_from(data.len())?.to_be_bytes());
    encoded[28..32].copy_from_slice(&crc32fast::hash(data).to_be_bytes());
    let header_crc = crc32fast::hash(&encoded[..28]);
    encoded[32..36].copy_from_slice(&header_crc.to_be_bytes());
    encoded[header_len..data_end].copy_from_slice(data);

    let decoded = crate::pitr::decode_v5_batch(&encoded, 0, LIVE_WAL_V5_LIMITS)?;
    ensure!(
        decoded.data_end == data_end && decoded.logical_end == aligned_len,
        "model RFC 023 decoder consumed an unexpected entry-stream length"
    );
    ensure!(
        decoded.batch.commit_ts == header.commit_ts
            && decoded.batch.recorded_at.secs == header.recorded_at_secs
            && decoded.batch.recorded_at.nanos == header.recorded_at_nanos
            && decoded.batch.entries.len() == usize::try_from(header.entry_count)?,
        "model RFC 023 decoded batch metadata disagrees with v7 header"
    );
    Ok(decoded.batch)
}

fn validate_recorded_time_order(
    previous: &mut Option<RecordedAt>,
    current: RecordedAt,
) -> Result<()> {
    if let Some(previous) = *previous {
        ensure!(current >= previous, "model WAL recorded times regress");
    }
    *previous = Some(current);
    Ok(())
}

fn verify_boundary_contents(
    frontier: WalV7Frontier,
    expected_ticket: u64,
    last_commit_ts: u64,
    last_data_end: u64,
    previous_frontier_offset: u64,
    logical_digest: [u8; 32],
) -> Result<()> {
    let previous_frontier_end = previous_frontier_offset
        .checked_add(WAL_V7_FRAME_LEN as u64)
        .ok_or_else(|| anyhow::anyhow!("model previous FRONTIER end overflows"))?;
    ensure!(
        frontier.ticket_end == expected_ticket
            && frontier.last_commit_ts == last_commit_ts
            && frontier.prefix_digest == logical_digest
            && frontier.durable_end == last_data_end.max(previous_frontier_end),
        "model FRONTIER does not describe the verified logical prefix"
    );
    Ok(())
}

/// Verifies a logical Active anchor as an exact prefix of the installed WAL.
/// The anchor may precede the newest frontier, so validation replays through
/// its `durable_end` rather than comparing it only to the physical tail.
fn verify_active_anchor(image: &[u8], anchor: ActiveAnchor) -> Result<()> {
    let header = WalV7Header::decode(image)?;
    ensure!(
        header.segment_id == anchor.segment_id && header.incarnation == anchor.incarnation,
        "model Active anchor identifies a different WAL"
    );
    let header_bytes = header.encode()?;
    let header_digest = header.digest()?;
    let mut logical_hash = Sha256::new();
    logical_hash.update(header_bytes);

    let generation_zero_offset = WAL_V7_HEADER_LEN as u64;
    let generation_zero_frame = frame_at(image, generation_zero_offset)?;
    let generation_zero_decoded = decode_frame_structural(
        generation_zero_frame,
        generation_zero_offset,
        &header_digest,
    )?;
    ensure!(
        generation_zero_decoded.kind == WalV7FrameKind::Frontier,
        "model Active anchor WAL has no generation-zero frontier"
    );
    let mut previous_frontier = WalV7Frontier::decode_body(&generation_zero_decoded.body)?;
    previous_frontier.validate_generation_zero(&header_digest, generation_zero_offset)?;
    let mut previous_frontier_offset = generation_zero_offset;
    let mut previous_frontier_digest = frame_record_digest(generation_zero_frame)?;
    let mut expected_ticket = 0_u64;
    let mut last_commit_ts = 0_u64;
    let mut last_recorded_at = None;
    let mut last_data_end = WAL_V7_HEADER_LEN as u64;

    if anchor.ticket_end == 0 {
        ensure!(
            anchor.durable_end == WAL_V7_HEADER_LEN as u64
                && anchor.last_commit_ts == 0
                && anchor.prefix_digest == header_digest,
            "model empty Active anchor does not match generation zero"
        );
        return Ok(());
    }
    ensure!(
        anchor.durable_end >= FIRST_DATA_OFFSET,
        "model nonempty Active anchor ends before DATA"
    );

    let mut physical_offset = FIRST_DATA_OFFSET;
    while physical_offset < anchor.durable_end {
        let frame = frame_at(image, physical_offset)?;
        let decoded = decode_frame_structural(frame, physical_offset, &header_digest)?;
        match decoded.kind {
            WalV7FrameKind::Data => {
                let (logical_header, batch_bytes, batch, next_offset) = decode_model_batch(
                    image,
                    physical_offset,
                    anchor.durable_end,
                    expected_ticket,
                    last_commit_ts,
                    &header_digest,
                )?;
                validate_recorded_time_order(&mut last_recorded_at, batch.recorded_at)?;
                logical_hash.update(&batch_bytes);
                expected_ticket = expected_ticket
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("model anchor ticket count overflows"))?;
                last_commit_ts = logical_header.commit_ts;
                last_data_end = next_offset;
                physical_offset = next_offset;
            }
            WalV7FrameKind::Frontier => {
                let (frontier, digest) = decode_frontier_successor(
                    frame,
                    physical_offset,
                    &header_digest,
                    previous_frontier,
                    previous_frontier_offset,
                    &previous_frontier_digest,
                )?;
                verify_boundary_contents(
                    frontier,
                    expected_ticket,
                    last_commit_ts,
                    last_data_end,
                    previous_frontier_offset,
                    logical_hash.clone().finalize().into(),
                )?;
                previous_frontier = frontier;
                previous_frontier_offset = physical_offset;
                previous_frontier_digest = digest;
                physical_offset = physical_offset
                    .checked_add(WAL_V7_FRAME_LEN as u64)
                    .ok_or_else(|| anyhow::anyhow!("model anchor frame offset overflows"))?;
            }
        }
    }
    ensure!(
        physical_offset == anchor.durable_end,
        "model Active anchor ends inside a frame or batch"
    );
    let previous_frontier_end = previous_frontier_offset
        .checked_add(WAL_V7_FRAME_LEN as u64)
        .ok_or_else(|| anyhow::anyhow!("model anchor frontier end overflows"))?;
    let anchor_digest: [u8; 32] = logical_hash.finalize().into();
    ensure!(
        anchor.ticket_end == expected_ticket
            && anchor.durable_end == last_data_end.max(previous_frontier_end)
            && anchor.last_commit_ts == last_commit_ts
            && anchor.prefix_digest == anchor_digest,
        "model Active anchor does not match its verified WAL prefix"
    );
    Ok(())
}

fn frame_at(image: &[u8], offset: u64) -> Result<&[u8]> {
    let start = usize::try_from(offset)?;
    let end = start
        .checked_add(WAL_V7_FRAME_LEN)
        .ok_or_else(|| anyhow::anyhow!("model frame range overflows"))?;
    ensure!(end <= image.len(), "model frame is incomplete");
    Ok(&image[start..end])
}

fn model_header() -> WalV7Header {
    let archive_epoch_id = ArchiveEpochId([0x31; 16]);
    WalV7Header {
        timeline_id: TimelineId([0x21; 16]),
        archive_epoch_id,
        segment_id: SegmentId(17),
        predecessor: ChainAnchor::Genesis { archive_epoch_id },
        incarnation: [0x41; 16],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame_kind_at(image: &[u8], offset: u64, digest: &[u8; 32]) -> Result<WalV7FrameKind> {
        Ok(decode_frame_structural(frame_at(image, offset)?, offset, digest)?.kind)
    }

    fn plausible_marker_bytes(header_digest: [u8; 32]) -> Result<[u8; WAL_V7_FRAME_LEN]> {
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
        let frontier = WalV7Frontier {
            generation: 1,
            ticket_end: 1,
            durable_end: FIRST_DATA_OFFSET + WAL_V7_FRAME_LEN as u64,
            last_commit_ts: 1,
            prefix_digest: [0x91; 32],
            previous_frontier_offset: WAL_V7_HEADER_LEN as u64,
            previous_frontier_digest: generation_zero_digest,
        };
        encode_frontier_successor(
            frontier,
            generation_zero,
            WAL_V7_HEADER_LEN as u64,
            &generation_zero_digest,
            FIRST_DATA_OFFSET + WAL_V7_FRAME_LEN as u64,
            header_digest,
        )
    }

    #[test]
    fn creation_waits_for_file_directory_and_active_manifest_barriers() -> Result<()> {
        let header = model_header();
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
        let generation_zero = encode_generation_zero_frontier(header_digest)?;
        let mut image = Vec::with_capacity(WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN);
        image.extend_from_slice(&header_bytes);
        image.extend_from_slice(&generation_zero);

        let mut disk = PersistenceModel::default();
        let inode = disk.create_file(ACTIVE_WAL_PATH)?;
        disk.write_at(inode, 0, &image)?;
        disk.fdatasync_success(inode)?;
        assert_eq!(disk.path_inode(ACTIVE_WAL_PATH, ImageView::Stable), None);
        assert!(!disk.service_ready_for(ACTIVE_WAL_PATH, inode));

        disk.sync_directory_success();
        assert_eq!(
            disk.path_inode(ACTIVE_WAL_PATH, ImageView::Stable),
            Some(inode)
        );
        assert!(!disk.service_ready_for(ACTIVE_WAL_PATH, inode));

        disk.publish_active_anchor(ActiveAnchor {
            segment_id: header.segment_id,
            incarnation: header.incarnation,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
        });
        assert!(!disk.service_ready_for(ACTIVE_WAL_PATH, inode));
        disk.sync_manifest_success();
        assert!(disk.service_ready_for(ACTIVE_WAL_PATH, inode));
        assert_eq!(disk.directory_sync_attempts, 1);
        assert_eq!(disk.manifest_sync_attempts, 1);
        assert_eq!(
            disk.stable_active_anchor(),
            Some(ActiveAnchor {
                segment_id: header.segment_id,
                incarnation: header.incarnation,
                ticket_end: 0,
                durable_end: WAL_V7_HEADER_LEN as u64,
                last_commit_ts: 0,
                prefix_digest: header_digest,
            })
        );
        let mismatched_anchor = ActiveAnchor {
            segment_id: header.segment_id,
            incarnation: header.incarnation,
            ticket_end: 1,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
        };
        disk.publish_active_anchor(mismatched_anchor);
        disk.sync_manifest_success();
        assert!(!disk.service_ready_for(ACTIVE_WAL_PATH, inode));
        Ok(())
    }

    #[test]
    fn service_readiness_accepts_a_valid_anchor_behind_the_latest_frontier() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        let inode = harness.writer.inode;
        let anchor = harness
            .disk
            .stable_active_anchor()
            .expect("creation persisted an Active anchor");
        assert_eq!(anchor.ticket_end, 0);

        harness.commit(b"newer durable batch", 5)?;

        let stable = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("active WAL remains installed");
        assert_eq!(recover_latest(stable)?.frontier.ticket_end, 1);
        assert_eq!(harness.disk.stable_active_anchor(), Some(anchor));
        assert!(harness.disk.service_ready_for(ACTIVE_WAL_PATH, inode));
        Ok(())
    }

    #[test]
    fn recovery_rejects_regressing_recorded_times_and_falls_back() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        harness.commit(b"first", 10)?;
        let regressing_time = RecordedAt {
            secs: -124,
            nanos: 456,
        };
        assert!(
            harness
                .begin_commit_at(b"clock moved backward", 20, regressing_time)
                .is_err()
        );

        // Inject a malformed but internally checksummed image directly. A
        // valid writer must not publish or ACK this regressing-time batch.
        let pending = harness.begin_commit(b"clock moved backward", 20)?;
        assert_eq!(pending.data_offsets.len(), 1);
        let data = encode_model_entry_stream(b"clock moved backward", 20)?;
        let logical_header = WalV7LogicalBatchHeader {
            segment_ticket: pending.ticket,
            commit_ts: pending.commit_ts,
            recorded_at_secs: regressing_time.secs,
            recorded_at_nanos: regressing_time.nanos,
            entry_count: 1,
        }
        .encode(&data, LIVE_WAL_V5_LIMITS)?;
        let mut logical_batch = logical_header.to_vec();
        logical_batch.extend_from_slice(&data);
        let body = WalV7DataFragmentHeader {
            segment_ticket: pending.ticket,
            fragment_index: 0,
            fragment_count: 1,
            batch_bytes: u64::try_from(logical_batch.len())?,
        }
        .encode_body(&logical_batch)?;
        let data_frame =
            encode_data_frame(harness.writer.header_digest, pending.data_offsets[0], &body)?;
        harness.disk.write_at(
            harness.writer.inode,
            usize::try_from(pending.data_offsets[0])?,
            &data_frame,
        )?;
        let mut logical_hash = harness.writer.logical_hash.clone();
        logical_hash.update(&logical_batch);
        let untrusted_frontier = WalV7Frontier {
            prefix_digest: logical_hash.finalize().into(),
            ..pending.frontier
        };
        let frontier_frame = encode_frontier_successor(
            untrusted_frontier,
            harness.writer.previous_frontier,
            harness.writer.previous_frontier_offset,
            &harness.writer.previous_frontier_digest,
            pending.frontier_offset,
            harness.writer.header_digest,
        )?;
        harness.disk.write_at(
            harness.writer.inode,
            usize::try_from(pending.frontier_offset)?,
            &frontier_frame,
        )?;
        harness.disk.fdatasync_success(harness.writer.inode)?;
        assert_eq!(harness.history.acknowledged_tickets, [0]);

        let stable = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("active WAL is installed");
        let recovered = recover_latest(stable)?;
        assert_eq!(recovered.frontier.ticket_end, 1);
        assert_eq!(recovered.frontier.last_commit_ts, 10);
        assert!(
            harness
                .history
                .acknowledged_tickets
                .iter()
                .all(|ticket| *ticket < recovered.frontier.ticket_end)
        );
        assert!(
            verify_active_anchor(
                stable,
                ActiveAnchor {
                    segment_id: model_header().segment_id,
                    incarnation: model_header().incarnation,
                    ticket_end: untrusted_frontier.ticket_end,
                    durable_end: untrusted_frontier.durable_end,
                    last_commit_ts: untrusted_frontier.last_commit_ts,
                    prefix_digest: untrusted_frontier.prefix_digest,
                },
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn recovery_bounds_batch_declarations_before_allocating() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        harness.commit(b"acknowledged", 10)?;
        let pending = harness.begin_commit(b"malformed fixture", 20)?;
        assert_eq!(pending.data_offsets.len(), 1);
        let data_offset = pending.data_offsets[0];
        let capacity = WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY as u64;
        for (batch_bytes, prefix_end, expected_error) in [
            (
                u64::from(u32::MAX) + WAL_V7_LOGICAL_BATCH_HEADER_LEN as u64,
                pending.frontier.durable_end,
                "configured byte limit",
            ),
            (
                capacity + 1,
                pending.frontier.durable_end,
                "available covered prefix",
            ),
            (
                2 * capacity + 1,
                pending.frontier_offset + 2 * WAL_V7_FRAME_LEN as u64,
                "available covered prefix",
            ),
        ] {
            let body = WalV7DataFragmentHeader {
                segment_ticket: pending.ticket,
                fragment_index: 0,
                fragment_count: u32::try_from(batch_bytes.div_ceil(capacity))?,
                batch_bytes,
            }
            .encode_body(&[0; WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY])?;
            let frame = encode_data_frame(harness.writer.header_digest, data_offset, &body)?;
            harness
                .disk
                .write_at(harness.writer.inode, usize::try_from(data_offset)?, &frame)?;
            harness.disk.fdatasync_success(harness.writer.inode)?;

            let stable = harness
                .disk
                .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
                .expect("active WAL remains installed");
            let error = decode_model_batch(
                stable,
                data_offset,
                prefix_end,
                pending.ticket,
                harness.writer.last_commit_ts,
                &harness.writer.header_digest,
            )
            .expect_err("malformed length must be rejected before allocation");
            assert!(error.to_string().contains(expected_error));
            assert_eq!(recover_latest(stable)?.frontier.ticket_end, 1);
            assert_eq!(harness.history.acknowledged_tickets, [0]);
        }
        Ok(())
    }

    #[test]
    fn never_installed_creation_is_an_orphan_and_damaged_installed_f0_fails() -> Result<()> {
        let header = model_header();
        let header_bytes = header.encode()?;
        let generation_zero = encode_generation_zero_frontier(header.digest()?)?;
        let mut image = Vec::with_capacity(WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN);
        image.extend_from_slice(&header_bytes);
        image.extend_from_slice(&generation_zero);

        let mut disk = PersistenceModel::default();
        let orphan = disk.create_file(ACTIVE_WAL_PATH)?;
        disk.write_at(orphan, 0, &image)?;
        disk.fdatasync_success(orphan)?;
        disk.sync_directory_success();
        disk.power_loss();
        assert_eq!(
            disk.path_inode(ACTIVE_WAL_PATH, ImageView::Stable),
            Some(orphan)
        );
        assert_eq!(disk.stable_active_anchor(), None);
        assert_eq!(disk.cleanup_uninstalled_active_file()?, 0);
        assert_eq!(disk.path_inode(ACTIVE_WAL_PATH, ImageView::Cached), None);
        assert_eq!(
            disk.path_inode(ACTIVE_WAL_PATH, ImageView::Stable),
            Some(orphan)
        );
        assert_eq!(disk.file(orphan)?.stable, image);

        // Without the directory barrier, power loss restores the orphan's
        // old durable name and its inode must remain available for cleanup.
        disk.power_loss();
        assert_eq!(
            disk.path_inode(ACTIVE_WAL_PATH, ImageView::Cached),
            Some(orphan)
        );
        assert_eq!(disk.cleanup_uninstalled_active_file()?, 0);
        disk.sync_directory_success();
        assert_eq!(disk.cleanup_unreferenced_inodes(), 1);
        assert_eq!(disk.path_inode(ACTIVE_WAL_PATH, ImageView::Stable), None);
        disk.power_loss();
        assert_eq!(disk.path_inode(ACTIVE_WAL_PATH, ImageView::Cached), None);

        let installed = SerialHarness::create()?;
        let stable = installed
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("installed WAL is present");
        let mut damaged = stable.to_vec();
        damaged[WAL_V7_HEADER_LEN] ^= 0x80;
        assert!(recover_latest(&damaged).is_err());
        Ok(())
    }

    #[test]
    fn data_and_frontier_share_one_sync_before_publication_and_ack() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        let inode = harness.writer.inode;
        let syncs_before = harness.disk.file(inode)?.sync_attempts;
        let marker_value = plausible_marker_bytes(harness.writer.header_digest)?;

        let pending = harness.begin_commit(&marker_value, 11)?;
        let expected_frontier_offset = pending.frontier_offset;
        harness.finish_commit(pending)?;

        assert_eq!(harness.disk.file(inode)?.sync_attempts, syncs_before + 1);
        assert_eq!(harness.disk.file(inode)?.successful_syncs, syncs_before + 1);
        assert_eq!(harness.history.published_tickets, [0]);
        assert_eq!(harness.history.acknowledged_tickets, [0]);
        let stable = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("active WAL is installed");
        let recovered = recover_latest(stable)?;
        assert_eq!(recovered.frame_offset, expected_frontier_offset);
        assert_eq!(recovered.frontier.ticket_end, 1);
        assert_eq!(recovered.batches.len(), 1);
        assert_eq!(
            recovered.batches[0].entries,
            [WalEntry::Put {
                key: b"model-key".to_vec(),
                value: marker_value.to_vec(),
            }]
        );
        assert_eq!(
            frame_kind_at(
                stable,
                expected_frontier_offset,
                &harness.writer.header_digest
            )?,
            WalV7FrameKind::Frontier
        );
        Ok(())
    }

    #[test]
    fn successful_sync_survives_even_if_publication_and_ack_do_not_run() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        let pending = harness.begin_commit(b"synced, not acknowledged", 5)?;
        harness.sync_pending_commit(&pending)?;

        assert!(harness.history.published_tickets.is_empty());
        assert!(harness.history.acknowledged_tickets.is_empty());
        let stable = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("active WAL is installed");
        let recovered = recover_latest(stable)?;
        assert_eq!(recovered.frontier.ticket_end, 1);
        assert_eq!(recovered.frame_offset, pending.frontier_offset);
        Ok(())
    }

    #[test]
    fn serial_writer_keeps_one_pending_commit_until_publication() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        let pending = harness.begin_commit(b"A", 10)?;
        let cached_before = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
            .expect("active WAL is installed")
            .to_vec();

        assert!(harness.begin_commit(b"must not overwrite A", 20).is_err());
        assert_eq!(
            harness.disk.path_bytes(ACTIVE_WAL_PATH, ImageView::Cached),
            Some(cached_before.as_slice())
        );
        harness.sync_pending_commit(&pending)?;
        assert!(
            harness
                .begin_commit(b"must wait for publication", 20)
                .is_err()
        );
        assert!(harness.history.acknowledged_tickets.is_empty());
        harness.publish_commit(pending)?;
        harness.commit(b"B", 20)?;
        harness.disk.power_loss();

        let recovered = recover_latest(
            harness
                .disk
                .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
                .expect("active WAL survives power loss"),
        )?;
        assert_eq!(harness.history.acknowledged_tickets, [0, 1]);
        assert_eq!(recovered.frontier.ticket_end, 2);
        assert_eq!(recovered.batches.len(), 2);
        for (batch, value) in recovered.batches.iter().zip([b"A", b"B"]) {
            assert_eq!(
                batch.entries,
                [WalEntry::Put {
                    key: b"model-key".to_vec(),
                    value: value.to_vec(),
                }]
            );
        }
        Ok(())
    }

    #[test]
    fn serial_writer_cannot_publish_before_its_sync() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        let pending = harness.begin_commit(b"unsynced", 10)?;
        let syncs_before = harness.disk.file(harness.writer.inode)?.sync_attempts;

        assert!(harness.publish_commit(pending).is_err());
        assert_eq!(
            harness.disk.file(harness.writer.inode)?.sync_attempts,
            syncs_before
        );
        assert!(harness.history.published_tickets.is_empty());
        assert!(harness.history.acknowledged_tickets.is_empty());
        harness.disk.power_loss();
        let stable = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("initial Active WAL is installed");
        assert_eq!(recover_latest(stable)?.frontier.ticket_end, 0);
        Ok(())
    }

    #[test]
    fn poisoned_serial_writer_cannot_finish_or_publish_a_failed_cycle() -> Result<()> {
        for publish_directly in [false, true] {
            let mut harness = SerialHarness::create()?;
            harness.commit(b"acknowledged", 10)?;
            let pending = harness.begin_commit(b"uncertain", 20)?;
            harness.interrupt_sync(&[])?;
            let syncs_before = harness.disk.file(harness.writer.inode)?.sync_attempts;

            let result = if publish_directly {
                harness.publish_commit(pending)
            } else {
                harness.finish_commit(pending)
            };
            assert!(result.is_err());
            assert_eq!(harness.writer.phase, CommitPhase::Poisoned);
            assert_eq!(
                harness.disk.file(harness.writer.inode)?.sync_attempts,
                syncs_before
            );
            assert_eq!(harness.history.published_tickets, [0]);
            assert_eq!(harness.history.acknowledged_tickets, [0]);
            assert!(harness.begin_commit(b"must not append", 30).is_err());
            let stable = harness
                .disk
                .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
                .expect("acknowledged Active prefix remains installed");
            assert_eq!(recover_latest(stable)?.frontier.ticket_end, 1);
        }
        Ok(())
    }

    #[test]
    fn replacement_retires_the_serial_writer_before_begin_sync_or_publication() -> Result<()> {
        for phase in [
            CommitPhase::Idle,
            CommitPhase::AwaitingSync { ticket: 1 },
            CommitPhase::Synced { ticket: 1 },
        ] {
            for sync_directory in [false, true] {
                let mut harness = SerialHarness::create()?;
                harness.commit(b"A", 10)?;
                let old_inode = harness.writer.inode;
                let image = harness
                    .disk
                    .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
                    .expect("acknowledged Active prefix is installed")
                    .to_vec();
                let pending = match phase {
                    CommitPhase::Idle => None,
                    CommitPhase::AwaitingSync { .. } => Some(harness.begin_commit(b"B", 20)?),
                    CommitPhase::Synced { .. } => {
                        let pending = harness.begin_commit(b"B", 20)?;
                        harness.sync_pending_commit(&pending)?;
                        Some(pending)
                    }
                    CommitPhase::Poisoned => unreachable!("fixture starts with a live cycle"),
                };

                let replacement = harness.disk.create_file(TEMP_WAL_PATH)?;
                harness.disk.write_at(replacement, 0, &image)?;
                harness.disk.fdatasync_success(replacement)?;
                harness
                    .disk
                    .rename_replace(TEMP_WAL_PATH, ACTIVE_WAL_PATH)?;
                if sync_directory {
                    harness.disk.sync_directory_success();
                }
                let syncs_before = harness.disk.file(old_inode)?.sync_attempts;
                let old_bytes = harness.disk.file(old_inode)?.cached.clone();

                let result = match (phase, pending) {
                    (CommitPhase::Idle, None) => harness.commit(b"B", 20),
                    (CommitPhase::AwaitingSync { .. }, Some(pending)) => {
                        harness.finish_commit(pending)
                    }
                    (CommitPhase::Synced { .. }, Some(pending)) => harness.publish_commit(pending),
                    _ => unreachable!("fixture retains its pending cycle"),
                };
                assert!(result.is_err());
                assert_eq!(harness.writer.phase, CommitPhase::Poisoned);
                assert_eq!(harness.disk.file(old_inode)?.sync_attempts, syncs_before);
                assert_eq!(harness.disk.file(old_inode)?.cached, old_bytes);
                assert_eq!(harness.history.published_tickets, [0]);
                assert_eq!(harness.history.acknowledged_tickets, [0]);

                harness.disk.power_loss();
                let recovered = recover_latest(
                    harness
                        .disk
                        .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
                        .expect("a durable canonical image survives"),
                )?;
                let expected_ticket_end =
                    if !sync_directory && matches!(phase, CommitPhase::Synced { .. }) {
                        2
                    } else {
                        1
                    };
                assert_eq!(recovered.frontier.ticket_end, expected_ticket_end);
                assert_eq!(
                    recovered.batches[0].entries,
                    [WalEntry::Put {
                        key: b"model-key".to_vec(),
                        value: b"A".to_vec(),
                    }]
                );
                assert!(
                    harness
                        .begin_commit(b"retired runtime must stay stopped", 30)
                        .is_err()
                );
            }
        }
        Ok(())
    }

    #[test]
    fn crashes_retire_old_serial_writers_and_pending_commits() -> Result<()> {
        #[derive(Clone, Copy)]
        enum Operation {
            Begin,
            Finish,
            Interrupt,
            Publish,
        }

        for operation in [
            Operation::Begin,
            Operation::Finish,
            Operation::Interrupt,
            Operation::Publish,
        ] {
            for power_loss in [false, true] {
                let mut harness = SerialHarness::create()?;
                harness.commit(b"A", 10)?;
                let inode = harness.writer.inode;
                let pending = match operation {
                    Operation::Begin => None,
                    Operation::Finish | Operation::Interrupt => {
                        Some(harness.begin_commit(b"B", 20)?)
                    }
                    Operation::Publish => {
                        let pending = harness.begin_commit(b"B", 20)?;
                        harness.sync_pending_commit(&pending)?;
                        Some(pending)
                    }
                };
                if power_loss {
                    harness.disk.power_loss();
                } else {
                    harness.disk.restart_process_without_reboot();
                }
                // The same canonical inode survives; only process lifetime
                // can revoke this old runtime's authority to sync or ACK.
                assert!(harness.disk.canonical_inode_matches(ACTIVE_WAL_PATH, inode));
                let syncs_before = harness.disk.file(inode)?.sync_attempts;
                let cached_after_crash = harness.disk.file(inode)?.cached.clone();

                let result = match operation {
                    Operation::Begin => harness.commit(b"B", 20),
                    Operation::Finish => {
                        harness.finish_commit(pending.expect("finish has a pending commit"))
                    }
                    Operation::Interrupt => harness.interrupt_sync(&[]),
                    Operation::Publish => {
                        harness.publish_commit(pending.expect("publish has a synced commit"))
                    }
                };
                assert!(result.is_err());
                assert_eq!(harness.writer.phase, CommitPhase::Poisoned);
                assert_eq!(harness.disk.file(inode)?.sync_attempts, syncs_before);
                assert_eq!(harness.disk.file(inode)?.cached, cached_after_crash);
                assert_eq!(harness.history.published_tickets, [0]);
                assert_eq!(harness.history.acknowledged_tickets, [0]);

                let cached = recover_latest(
                    harness
                        .disk
                        .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
                        .expect("canonical Active image survives the crash"),
                )?;
                let stable = recover_latest(
                    harness
                        .disk
                        .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
                        .expect("acknowledged Active prefix remains durable"),
                )?;
                let synced_b = matches!(operation, Operation::Publish);
                let cached_b = synced_b || (!power_loss && !matches!(operation, Operation::Begin));
                assert_eq!(cached.frontier.ticket_end, if cached_b { 2 } else { 1 });
                assert_eq!(stable.frontier.ticket_end, if synced_b { 2 } else { 1 });
                assert_eq!(
                    cached.batches[0].entries,
                    [WalEntry::Put {
                        key: b"model-key".to_vec(),
                        value: b"A".to_vec(),
                    }]
                );
                assert!(
                    harness
                        .begin_commit(b"old runtime must stay retired", 30)
                        .is_err()
                );
            }
        }
        Ok(())
    }

    #[test]
    fn interrupted_sync_can_leave_a_stable_marker_without_its_data() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        harness.commit(b"acknowledged", 10)?;
        let pending = harness.begin_commit(b"uncertain", 20)?;
        let data_offset = pending.data_offsets[0];
        let frontier_offset = pending.frontier_offset;
        let marker_range = usize::try_from(frontier_offset)?
            ..usize::try_from(frontier_offset + WAL_V7_FRAME_LEN as u64)?;
        harness.interrupt_sync(&[marker_range])?;

        assert_eq!(harness.history.acknowledged_tickets, [0]);
        assert_eq!(harness.history.published_tickets, [0]);
        assert!(harness.begin_commit(b"must not append", 30).is_err());

        let stable = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("active WAL is installed");
        assert_eq!(
            frame_kind_at(stable, frontier_offset, &harness.writer.header_digest)?,
            WalV7FrameKind::Frontier
        );
        assert!(frame_at(stable, data_offset).is_ok_and(|frame| {
            decode_frame_structural(frame, data_offset, &harness.writer.header_digest).is_err()
        }));
        let recovered_stable = recover_latest(stable)?;
        assert_eq!(recovered_stable.frontier.ticket_end, 1);
        assert_eq!(
            recovered_stable.frame_offset,
            WAL_V7_HEADER_LEN as u64 + 2 * WAL_V7_FRAME_LEN as u64
        );

        // A process restart keeps the page cache, so recovery may observe the
        // complete but unacknowledged candidate. The ACK oracle is independent.
        harness.disk.restart_process_without_reboot();
        assert_eq!(harness.disk.process_restarts, 1);
        assert!(
            !harness
                .disk
                .service_ready_for(ACTIVE_WAL_PATH, harness.writer.inode)
        );
        let cached = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
            .expect("active WAL remains visible after process restart");
        let recovered_cached = recover_latest(cached)?;
        assert_eq!(recovered_cached.frontier.ticket_end, 2);
        assert_eq!(recovered_cached.frame_offset, frontier_offset);
        assert_eq!(harness.history.acknowledged_tickets, [0]);

        // A power loss discards the unsynced DATA and returns to the older
        // certificate, while retaining the marker bytes that reached storage.
        harness.disk.power_loss();
        let after_power_loss = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
            .expect("active path survives power loss");
        assert_eq!(recover_latest(after_power_loss)?.frontier.ticket_end, 1);
        assert_eq!(harness.history.acknowledged_tickets, [0]);
        Ok(())
    }

    #[test]
    fn fresh_inode_replacement_is_not_durable_until_directory_sync() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        let old_inode = harness.writer.inode;
        let image = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("active WAL is installed")
            .to_vec();

        let first_temp = harness.disk.create_file(TEMP_WAL_PATH)?;
        harness.disk.write_at(first_temp, 0, &image)?;
        harness.disk.fdatasync_success(first_temp)?;
        harness
            .disk
            .rename_replace(TEMP_WAL_PATH, ACTIVE_WAL_PATH)?;
        assert_eq!(
            harness.disk.path_inode(ACTIVE_WAL_PATH, ImageView::Cached),
            Some(first_temp)
        );
        assert!(!harness.disk.service_ready_for(ACTIVE_WAL_PATH, first_temp));
        assert!(!harness.disk.service_ready_for(ACTIVE_WAL_PATH, old_inode));
        harness.disk.restart_process_without_reboot();
        assert!(!harness.disk.service_ready_for(ACTIVE_WAL_PATH, first_temp));
        assert!(!harness.disk.service_ready_for(ACTIVE_WAL_PATH, old_inode));

        harness.disk.power_loss();
        assert_eq!(
            harness.disk.path_inode(ACTIVE_WAL_PATH, ImageView::Cached),
            Some(old_inode)
        );
        assert!(harness.disk.service_ready_for(ACTIVE_WAL_PATH, old_inode));
        assert_eq!(harness.disk.cleanup_unreferenced_inodes(), 1);

        let second_temp = harness.disk.create_file(TEMP_WAL_PATH)?;
        harness.disk.write_at(second_temp, 0, &image)?;
        harness.disk.fdatasync_success(second_temp)?;
        harness
            .disk
            .rename_replace(TEMP_WAL_PATH, ACTIVE_WAL_PATH)?;
        assert!(!harness.disk.service_ready_for(ACTIVE_WAL_PATH, second_temp));
        assert!(!harness.disk.service_ready_for(ACTIVE_WAL_PATH, old_inode));
        harness.disk.sync_directory_success();
        assert!(harness.disk.service_ready_for(ACTIVE_WAL_PATH, second_temp));
        assert!(!harness.disk.service_ready_for(ACTIVE_WAL_PATH, old_inode));
        assert_eq!(harness.disk.cleanup_unreferenced_inodes(), 1);
        harness.disk.power_loss();
        assert_eq!(
            harness.disk.path_inode(ACTIVE_WAL_PATH, ImageView::Stable),
            Some(second_temp)
        );
        assert!(harness.disk.service_ready_for(ACTIVE_WAL_PATH, second_temp));
        Ok(())
    }
}
