//! Deterministic persistence model for the v7 codec and one-sync protocol.
//!
//! This is a test oracle, not a production recovery implementation. Its small
//! scanner deliberately supports single-writer batches while exercising the
//! on-disk codec, stable/cached persistence states, and the RFC's certificate
//! boundaries.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

use super::admission::{
    WalV7Reservation, WalV7ReservationError, WalV7ResourceCharge, WalV7ResourceLedger,
    WalV7ResourceLimits,
};
use super::codec::{
    WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY, WAL_V7_FRAME_HEADER_LEN, WAL_V7_FRAME_LEN,
    WAL_V7_HEADER_LEN, WAL_V7_LOGICAL_BATCH_HEADER_LEN, WalV7DataFragmentHeader, WalV7FrameKind,
    WalV7Frontier, WalV7Header, WalV7LogicalBatchHeader, data_fragment_count,
    decode_frame_structural, decode_frontier_successor, encode_data_frame,
    encode_frontier_successor, encode_generation_zero_frontier, frame_record_digest,
};
use super::install::WalV7InstalledRecovery;
use crate::pitr::{
    ArchiveEpochId, ChainAnchor, LIVE_WAL_V5_LIMITS, RecordedAt, SegmentId, TimelineId, WalBatch,
    WalEntry, manifest::ActiveBoundary,
};

const ACTIVE_WAL_PATH: &str = "active.wal";
const TEMP_WAL_PATH: &str = "active.wal.tmp";
const FIRST_DATA_OFFSET: u64 = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64;

fn model_resource_limits() -> WalV7ResourceLimits {
    WalV7ResourceLimits {
        max_logical_wal_bytes: u64::MAX,
        max_source_spool_bytes: u64::MAX,
        max_recovery_workspace_bytes: 0,
        max_buffer_memory_bytes: u64::MAX,
        max_seal_index_bytes: u64::MAX,
    }
}

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
    next_physical_hash: Sha256,
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
    physical_hash: Sha256,
    seal_index: Vec<u64>,
    previous_frontier: WalV7Frontier,
    previous_frontier_offset: u64,
    previous_frontier_digest: [u8; 32],
    next_ticket: u64,
    next_data_offset: u64,
    last_commit_ts: u64,
    last_recorded_at: Option<RecordedAt>,
    phase: CommitPhase,
    resource_ledger: WalV7ResourceLedger,
    pending_reservation: Option<WalV7Reservation>,
    retained_reservations: Vec<WalV7Reservation>,
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
        Self::create_with_resource_limits(model_resource_limits())
    }

    fn create_with_resource_limits(limits: WalV7ResourceLimits) -> Result<Self> {
        let resource_ledger = WalV7ResourceLedger::new(limits)?;
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
        let mut physical_hash = Sha256::new();
        physical_hash.update(header.encode()?);
        physical_hash.update(generation_zero_frame);
        let process_generation = disk.process_generation;
        Ok(Self {
            disk,
            writer: SerialWriter {
                inode,
                process_generation,
                header_digest,
                logical_hash,
                physical_hash,
                seal_index: Vec::new(),
                previous_frontier: generation_zero,
                previous_frontier_offset: WAL_V7_HEADER_LEN as u64,
                previous_frontier_digest: generation_zero_digest,
                next_ticket: 0,
                next_data_offset: FIRST_DATA_OFFSET,
                last_commit_ts: 0,
                last_recorded_at: None,
                phase: CommitPhase::Idle,
                resource_ledger,
                pending_reservation: None,
                retained_reservations: Vec::new(),
            },
            history: WriteHistory::default(),
        })
    }

    fn from_installed_recovery(
        image: &[u8],
        installed: WalV7InstalledRecovery,
        acknowledged_tickets: Vec<u64>,
        limits: WalV7ResourceLimits,
    ) -> Result<Self> {
        ensure!(
            u64::try_from(image.len())? == installed.image_len,
            "installed v7 image length does not match recovered state"
        );
        ensure!(
            installed.active_boundary.ticket_end == installed.next_ticket
                && installed.frontier.ticket_end == installed.next_ticket
                && u64::try_from(installed.batches.len())? == installed.next_ticket
                && u64::try_from(installed.seal_index.len())? == installed.next_ticket,
            "installed v7 state has inconsistent ticket metadata"
        );
        ensure!(
            image.len() >= WAL_V7_HEADER_LEN,
            "installed v7 image is shorter than its header"
        );
        let header_bytes: [u8; WAL_V7_HEADER_LEN] = image[..WAL_V7_HEADER_LEN].try_into()?;
        let header = WalV7Header::decode(&header_bytes)?;
        let header_digest = header.digest()?;
        ensure!(
            installed.active_boundary.timeline_id == header.timeline_id.0
                && installed.active_boundary.archive_epoch_id == header.archive_epoch_id.0
                && installed.active_boundary.segment_id == header.segment_id.0
                && installed.active_boundary.incarnation == header.incarnation,
            "installed v7 state identifies a different WAL header"
        );
        let logical_digest: [u8; 32] = installed.logical_hasher.clone().finalize().into();
        ensure!(
            installed.active_boundary.prefix_digest == logical_digest,
            "installed v7 logical hash does not match its Active boundary"
        );
        let image_digest: [u8; 32] = Sha256::digest(image).into();
        ensure!(
            installed.image_digest() == image_digest,
            "installed v7 physical hash does not match its image"
        );

        let resource_ledger = WalV7ResourceLedger::new(limits)?;
        // Startup headroom owns the immutable header and generation-zero
        // frame. Recovered usage owns only the retained DATA/control extent,
        // without recreating unused per-batch marker or buffer reservations.
        let wal_bytes = installed
            .image_len
            .checked_sub(FIRST_DATA_OFFSET)
            .ok_or_else(|| {
                anyhow::anyhow!("installed model image is missing its initial frames")
            })?;
        let seal_index_bytes = u64::try_from(installed.seal_index.len())?
            .checked_mul(std::mem::size_of::<u64>() as u64)
            .ok_or_else(|| anyhow::anyhow!("installed model seal index size overflows"))?;
        let recovered_charge = WalV7ResourceCharge {
            logical_wal_bytes: wal_bytes,
            source_spool_bytes: wal_bytes,
            seal_index_bytes,
            ..WalV7ResourceCharge::default()
        };
        let mut retained_reservations = Vec::new();
        if recovered_charge != WalV7ResourceCharge::default() {
            retained_reservations.push(resource_ledger.reserve(recovered_charge)?);
        }

        let mut disk = PersistenceModel::default();
        let inode = disk.create_file(ACTIVE_WAL_PATH)?;
        disk.write_at(inode, 0, image)?;
        disk.fdatasync_success(inode)?;
        disk.sync_directory_success();
        disk.publish_active_anchor(model_anchor(installed.active_boundary));
        disk.sync_manifest_success();

        let process_generation = disk.process_generation;
        let next_ticket = installed.next_ticket;
        let last_commit_ts = installed.active_boundary.last_commit_ts;
        let published_tickets: Vec<_> = (0..next_ticket).collect();
        ensure!(
            acknowledged_tickets
                .iter()
                .all(|ticket| *ticket < next_ticket)
                && acknowledged_tickets
                    .windows(2)
                    .all(|pair| pair[0] < pair[1]),
            "recovery model ACK history is not an ordered subset of the installed prefix"
        );
        Ok(Self {
            disk,
            writer: SerialWriter {
                inode,
                process_generation,
                header_digest,
                logical_hash: installed.logical_hasher,
                physical_hash: installed.physical_hasher,
                seal_index: installed.seal_index,
                previous_frontier: installed.frontier,
                previous_frontier_offset: installed.frontier_offset,
                previous_frontier_digest: installed.frontier_digest,
                next_ticket,
                next_data_offset: installed.append_offset,
                last_commit_ts,
                last_recorded_at: installed.last_recorded_at,
                phase: CommitPhase::Idle,
                resource_ledger,
                pending_reservation: None,
                retained_reservations,
            },
            history: WriteHistory {
                published_tickets,
                acknowledged_tickets,
            },
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

        let entry_stream = encode_model_entry_stream(data, commit_ts)?;
        let batch_bytes_len = WAL_V7_LOGICAL_BATCH_HEADER_LEN
            .checked_add(entry_stream.len())
            .ok_or_else(|| anyhow::anyhow!("model logical batch length overflows"))?;
        let batch_bytes_len_u64 = u64::try_from(batch_bytes_len)?;
        let fragment_count = data_fragment_count(batch_bytes_len_u64).ok_or_else(|| {
            anyhow::anyhow!("model logical batch length is outside v7 wire bounds")
        })?;
        let fragment_capacity = usize::try_from(fragment_count)?;
        ensure!(
            self.writer.pending_reservation.is_none(),
            "v7 serial writer has a stale batch reservation"
        );
        let reservation = self
            .writer
            .resource_ledger
            .reserve_batch(batch_bytes_len_u64)?;
        let ticket = self.writer.next_ticket;
        let batch_header = WalV7LogicalBatchHeader {
            segment_ticket: ticket,
            commit_ts,
            recorded_at_secs: recorded_at.secs,
            recorded_at_nanos: recorded_at.nanos,
            entry_count: 1,
        }
        .encode(&entry_stream, LIVE_WAL_V5_LIMITS)?;
        self.writer.pending_reservation = Some(reservation);
        let mut logical_batch = Vec::with_capacity(batch_bytes_len);
        logical_batch.extend_from_slice(&batch_header);
        logical_batch.extend_from_slice(&entry_stream);

        let mut data_offsets = Vec::with_capacity(fragment_capacity);
        let mut next_physical_hash = self.writer.physical_hash.clone();
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
            next_physical_hash.update(frame);
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
        next_physical_hash.update(frontier_frame);
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
            next_physical_hash,
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
        let first_data_offset = *pending
            .data_offsets
            .first()
            .ok_or_else(|| anyhow::anyhow!("model published batch has no DATA frame"))?;
        let reservation =
            self.writer.pending_reservation.as_mut().ok_or_else(|| {
                anyhow::anyhow!("model published batch is missing its reservation")
            })?;
        self.writer
            .resource_ledger
            .release_buffer_memory(reservation)?;
        let reservation =
            self.writer.pending_reservation.take().ok_or_else(|| {
                anyhow::anyhow!("model published batch is missing its reservation")
            })?;
        self.writer.logical_hash = pending.next_logical_hash;
        self.writer.physical_hash = pending.next_physical_hash;
        self.writer.seal_index.push(first_data_offset);
        self.writer.previous_frontier = pending.frontier;
        self.writer.previous_frontier_offset = pending.frontier_offset;
        self.writer.previous_frontier_digest = frontier_digest;
        self.writer.next_ticket = pending.frontier.ticket_end;
        self.writer.next_data_offset = next_data_offset;
        self.writer.last_commit_ts = pending.commit_ts;
        self.writer.last_recorded_at = Some(pending.recorded_at);
        self.history.published_tickets.push(pending.ticket);
        self.history.acknowledged_tickets.push(pending.ticket);
        self.writer.retained_reservations.push(reservation);
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
    Ok(v5_entry_stream(&encoded)?.to_vec())
}

fn v5_entry_stream(encoded_batch: &[u8]) -> Result<&[u8]> {
    const DATA_LEN_OFFSET: usize = 24;

    let data_len_end = DATA_LEN_OFFSET + std::mem::size_of::<u32>();
    ensure!(
        encoded_batch.len() >= data_len_end,
        "model v5 batch header is truncated"
    );
    let data_len = usize::try_from(u32::from_be_bytes(
        encoded_batch[DATA_LEN_OFFSET..data_len_end].try_into()?,
    ))?;
    let data_start = crate::pitr::WAL_V5_BATCH_HEADER_LEN;
    let data_end = data_start
        .checked_add(data_len)
        .ok_or_else(|| anyhow::anyhow!("model v5 entry stream range overflows"))?;
    ensure!(
        data_end <= encoded_batch.len(),
        "model v5 entry stream is truncated"
    );
    Ok(&encoded_batch[data_start..data_end])
}

/// Use the shared RFC 023 entry-stream decoder after the v7 logical header has
/// validated its own length and checksum fields.
fn decode_model_entry_stream(header: WalV7LogicalBatchHeader, data: &[u8]) -> Result<WalBatch> {
    crate::pitr::decode_wal_entry_stream(
        header.commit_ts,
        RecordedAt {
            secs: header.recorded_at_secs,
            nanos: header.recorded_at_nanos,
        },
        header.entry_count,
        data,
        LIVE_WAL_V5_LIMITS,
    )
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

fn model_anchor(boundary: ActiveBoundary) -> ActiveAnchor {
    ActiveAnchor {
        segment_id: SegmentId(boundary.segment_id),
        incarnation: boundary.incarnation,
        ticket_end: boundary.ticket_end,
        durable_end: boundary.durable_end,
        last_commit_ts: boundary.last_commit_ts,
        prefix_digest: boundary.prefix_digest,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::{Read, Seek, SeekFrom},
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::super::{
        install::install_active_recovery,
        recovery::{
            WalV7RecoveryAuthority, discover_frontier_candidates, select_recovery_candidate,
        },
    };
    use super::*;

    static TEMP_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Result<Self> {
            let path = std::env::temp_dir().join(format!(
                "toy-kv-v7-model-{}-{}",
                std::process::id(),
                TEMP_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn active_boundary(frontier: WalV7Frontier) -> ActiveBoundary {
        let header = model_header();
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

    fn manifest_boundary(anchor: ActiveAnchor) -> ActiveBoundary {
        let header = model_header();
        ActiveBoundary {
            timeline_id: header.timeline_id.0,
            archive_epoch_id: header.archive_epoch_id.0,
            segment_id: anchor.segment_id.0,
            incarnation: anchor.incarnation,
            ticket_end: anchor.ticket_end,
            durable_end: anchor.durable_end,
            last_commit_ts: anchor.last_commit_ts,
            prefix_digest: anchor.prefix_digest,
        }
    }

    fn generation_zero(
        header_digest: [u8; 32],
    ) -> Result<([u8; WAL_V7_FRAME_LEN], WalV7Frontier, [u8; 32])> {
        let frame = encode_generation_zero_frontier(header_digest)?;
        let decoded = decode_frame_structural(&frame, WAL_V7_HEADER_LEN as u64, &header_digest)?;
        let frontier = WalV7Frontier::decode_body(&decoded.body)?;
        Ok((frame, frontier, frame_record_digest(&frame)?))
    }

    fn fixture_data_frames(
        header_digest: [u8; 32],
        frame_offset: u64,
        ticket: u64,
        commit_ts: u64,
        value: &[u8],
    ) -> Result<(Vec<[u8; WAL_V7_FRAME_LEN]>, Vec<u8>)> {
        let recorded_at = RecordedAt {
            secs: -123,
            nanos: 456,
        };
        let entry_stream = encode_model_entry_stream(value, commit_ts)?;
        let batch_header = WalV7LogicalBatchHeader {
            segment_ticket: ticket,
            commit_ts,
            recorded_at_secs: recorded_at.secs,
            recorded_at_nanos: recorded_at.nanos,
            entry_count: 1,
        }
        .encode(&entry_stream, LIVE_WAL_V5_LIMITS)?;
        let mut logical_batch = Vec::with_capacity(batch_header.len() + entry_stream.len());
        logical_batch.extend_from_slice(&batch_header);
        logical_batch.extend_from_slice(&entry_stream);
        let fragment_count = logical_batch
            .len()
            .div_ceil(WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY);
        let mut frames = Vec::new();
        frames.try_reserve_exact(fragment_count)?;
        for (fragment_index, payload) in logical_batch
            .chunks(WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY)
            .enumerate()
        {
            let fragment = WalV7DataFragmentHeader {
                segment_ticket: ticket,
                fragment_index: u32::try_from(fragment_index)?,
                fragment_count: u32::try_from(fragment_count)?,
                batch_bytes: u64::try_from(logical_batch.len())?,
            };
            let body = fragment.encode_body(payload)?;
            let offset = frame_offset
                .checked_add(
                    u64::try_from(fragment_index)?
                        .checked_mul(WAL_V7_FRAME_LEN as u64)
                        .ok_or_else(|| anyhow::anyhow!("fixture DATA offset overflows"))?,
                )
                .ok_or_else(|| anyhow::anyhow!("fixture DATA offset overflows"))?;
            frames.push(encode_data_frame(header_digest, offset, &body)?);
        }
        Ok((frames, logical_batch))
    }

    fn append_frames(image: &mut Vec<u8>, frames: &[[u8; WAL_V7_FRAME_LEN]]) {
        for frame in frames {
            image.extend_from_slice(frame);
        }
    }

    fn logical_prefix_digest(header: &[u8], batches: &[&[u8]]) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(header);
        for batch in batches {
            hash.update(batch);
        }
        hash.finalize().into()
    }

    fn active_selection(
        path: &Path,
        anchor: ActiveBoundary,
    ) -> Result<super::super::recovery::WalV7RecoverySelection> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        file.seek(SeekFrom::Start(0))?;
        let mut header_bytes = [0; WAL_V7_HEADER_LEN];
        file.read_exact(&mut header_bytes)?;
        let header = WalV7Header::decode(&header_bytes)?;
        let header_digest = header.digest()?;
        let discovery = discover_frontier_candidates(&mut file, file_len, &header_digest)?;
        select_recovery_candidate(
            &mut file,
            file_len,
            &discovery,
            WalV7RecoveryAuthority::Active {
                predecessor: header.predecessor,
                durable_anchors: vec![anchor],
            },
        )
    }

    fn empty_image_with_torn_tail() -> Result<(Vec<u8>, ActiveBoundary)> {
        let header = model_header();
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
        let (generation_zero_frame, generation_zero, _) = generation_zero(header_digest)?;
        let mut image = header_bytes.to_vec();
        image.extend_from_slice(&generation_zero_frame);
        image.extend_from_slice(b"partial tail");
        Ok((image, active_boundary(generation_zero)))
    }

    fn marker_behind_speculative_data_image() -> Result<(Vec<u8>, ActiveBoundary)> {
        let header = model_header();
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
        let (generation_zero_frame, generation_zero, generation_zero_digest) =
            generation_zero(header_digest)?;
        let mut image = header_bytes.to_vec();
        image.extend_from_slice(&generation_zero_frame);

        let (durable_data, durable_batch) =
            fixture_data_frames(header_digest, FIRST_DATA_OFFSET, 0, 10, b"durable")?;
        append_frames(&mut image, &durable_data);
        let data_end = FIRST_DATA_OFFSET
            .checked_add(u64::try_from(durable_data.len())? * WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("fixture durable DATA end overflows"))?;
        let (speculative_data, _) =
            fixture_data_frames(header_digest, data_end, 1, 20, b"speculative")?;
        append_frames(&mut image, &speculative_data);
        let speculative_end = data_end
            .checked_add(u64::try_from(speculative_data.len())? * WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("fixture speculative DATA end overflows"))?;
        let frontier = WalV7Frontier {
            generation: 1,
            ticket_end: 1,
            durable_end: data_end,
            last_commit_ts: 10,
            prefix_digest: logical_prefix_digest(&header_bytes, &[&durable_batch]),
            previous_frontier_offset: WAL_V7_HEADER_LEN as u64,
            previous_frontier_digest: generation_zero_digest,
        };
        let frontier_frame = encode_frontier_successor(
            frontier,
            generation_zero,
            WAL_V7_HEADER_LEN as u64,
            &generation_zero_digest,
            speculative_end,
            header_digest,
        )?;
        image.extend_from_slice(&frontier_frame);
        Ok((image, active_boundary(frontier)))
    }

    fn coalesced_group_image() -> Result<(Vec<u8>, ActiveBoundary)> {
        let header = model_header();
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
        let (generation_zero_frame, generation_zero, generation_zero_digest) =
            generation_zero(header_digest)?;
        let mut image = header_bytes.to_vec();
        image.extend_from_slice(&generation_zero_frame);
        let mut logical_hash = Sha256::new();
        logical_hash.update(header_bytes);
        for (ticket, value) in [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .enumerate()
        {
            let ticket = u64::try_from(ticket)?;
            let (frames, logical_batch) = fixture_data_frames(
                header_digest,
                u64::try_from(image.len())?,
                ticket,
                (ticket + 1) * 10,
                value,
            )?;
            append_frames(&mut image, &frames);
            logical_hash.update(logical_batch);
        }
        let frontier_offset = u64::try_from(image.len())?;
        let frontier = WalV7Frontier {
            generation: 1,
            ticket_end: 2,
            durable_end: frontier_offset,
            last_commit_ts: 20,
            prefix_digest: logical_hash.finalize().into(),
            previous_frontier_offset: WAL_V7_HEADER_LEN as u64,
            previous_frontier_digest: generation_zero_digest,
        };
        image.extend_from_slice(&encode_frontier_successor(
            frontier,
            generation_zero,
            WAL_V7_HEADER_LEN as u64,
            &generation_zero_digest,
            frontier_offset,
            header_digest,
        )?);
        Ok((image, active_boundary(frontier)))
    }

    fn trailing_control_image() -> Result<(Vec<u8>, ActiveBoundary)> {
        let header = model_header();
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
        let (generation_zero_frame, generation_zero, generation_zero_digest) =
            generation_zero(header_digest)?;
        let mut image = header_bytes.to_vec();
        image.extend_from_slice(&generation_zero_frame);

        let (first_data, first_batch) =
            fixture_data_frames(header_digest, FIRST_DATA_OFFSET, 0, 10, b"first")?;
        append_frames(&mut image, &first_data);
        let first_data_end = FIRST_DATA_OFFSET
            .checked_add(u64::try_from(first_data.len())? * WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("fixture first DATA end overflows"))?;
        let (second_data, second_batch) =
            fixture_data_frames(header_digest, first_data_end, 1, 20, b"second")?;
        append_frames(&mut image, &second_data);
        let second_data_end = first_data_end
            .checked_add(u64::try_from(second_data.len())? * WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("fixture second DATA end overflows"))?;

        // The first certificate trails ticket 1's speculative DATA physically,
        // but still covers only ticket 0. The next certificate covers both
        // DATA batches and the first marker as a trailing control frame.
        let first_frontier = WalV7Frontier {
            generation: 1,
            ticket_end: 1,
            durable_end: first_data_end,
            last_commit_ts: 10,
            prefix_digest: logical_prefix_digest(&header_bytes, &[&first_batch]),
            previous_frontier_offset: WAL_V7_HEADER_LEN as u64,
            previous_frontier_digest: generation_zero_digest,
        };
        let first_frontier_frame = encode_frontier_successor(
            first_frontier,
            generation_zero,
            WAL_V7_HEADER_LEN as u64,
            &generation_zero_digest,
            second_data_end,
            header_digest,
        )?;
        let first_frontier_digest = frame_record_digest(&first_frontier_frame)?;
        image.extend_from_slice(&first_frontier_frame);
        let first_frontier_end = second_data_end
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .ok_or_else(|| anyhow::anyhow!("fixture first FRONTIER end overflows"))?;
        let second_frontier = WalV7Frontier {
            generation: 2,
            ticket_end: 2,
            durable_end: first_frontier_end.max(second_data_end),
            last_commit_ts: 20,
            prefix_digest: logical_prefix_digest(&header_bytes, &[&first_batch, &second_batch]),
            previous_frontier_offset: second_data_end,
            previous_frontier_digest: first_frontier_digest,
        };
        let second_frontier_frame = encode_frontier_successor(
            second_frontier,
            first_frontier,
            second_data_end,
            &first_frontier_digest,
            first_frontier_end,
            header_digest,
        )?;
        image.extend_from_slice(&second_frontier_frame);
        Ok((image, active_boundary(second_frontier)))
    }

    fn assert_batch_values(batches: &[WalBatch], expected_values: &[&[u8]]) {
        assert_eq!(batches.len(), expected_values.len());
        for (batch, value) in batches.iter().zip(expected_values) {
            assert_eq!(
                batch.entries,
                [WalEntry::Put {
                    key: b"model-key".to_vec(),
                    value: value.to_vec(),
                }]
            );
        }
    }

    fn normalize_twice_continue_and_reopen(
        image: Vec<u8>,
        original_anchor: ActiveBoundary,
        expected_values: &[&[u8]],
        prior_acknowledged_tickets: &[u64],
    ) -> Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.0.join("active.wal");
        fs::write(&path, &image)?;

        let first_selection = active_selection(&path, original_anchor)?;
        assert_eq!(
            first_selection.active_boundary.ticket_end,
            u64::try_from(expected_values.len())?
        );
        let first_install = install_active_recovery(&path, &first_selection)?;
        let first_image = fs::read(&path)?;
        let first_image_digest: [u8; 32] = Sha256::digest(&first_image).into();
        assert_eq!(first_install.image_len, u64::try_from(first_image.len())?);
        assert_eq!(first_install.image_digest(), first_image_digest);
        assert_batch_values(&first_install.batches, expected_values);

        let second_selection = active_selection(&path, first_install.active_boundary)?;
        let second_install = install_active_recovery(&path, &second_selection)?;
        let normalized_image = fs::read(&path)?;
        assert_eq!(normalized_image, first_image);
        assert_eq!(
            second_install.active_boundary,
            first_install.active_boundary
        );
        assert_eq!(second_install.frontier, first_install.frontier);
        assert_eq!(
            second_install.frontier_offset,
            first_install.frontier_offset
        );
        assert_eq!(
            second_install.frontier_digest,
            first_install.frontier_digest
        );
        assert_eq!(second_install.append_offset, first_install.append_offset);
        assert_eq!(second_install.next_ticket, first_install.next_ticket);
        assert_eq!(
            second_install.last_recorded_at,
            first_install.last_recorded_at
        );
        assert_eq!(second_install.batches, first_install.batches);
        assert_eq!(second_install.seal_index, first_install.seal_index);
        assert_eq!(second_install.image_digest(), first_install.image_digest());
        let expected_wal_bytes =
            u64::try_from(normalized_image.len() - WAL_V7_HEADER_LEN - WAL_V7_FRAME_LEN)?;
        let expected_resource_charge = WalV7ResourceCharge {
            logical_wal_bytes: expected_wal_bytes,
            source_spool_bytes: expected_wal_bytes,
            recovery_workspace_bytes: 0,
            buffer_memory_bytes: 0,
            seal_index_bytes: u64::try_from(first_install.seal_index.len())?
                * std::mem::size_of::<u64>() as u64,
        };
        let next_ticket = second_install.next_ticket;
        let next_offset = second_install.append_offset;
        let last_commit_ts = second_install.active_boundary.last_commit_ts;
        let mut harness = SerialHarness::from_installed_recovery(
            &normalized_image,
            second_install,
            prior_acknowledged_tickets.to_vec(),
            WalV7ResourceLimits {
                max_buffer_memory_bytes: WAL_V7_FRAME_LEN as u64,
                ..model_resource_limits()
            },
        )?;
        assert_eq!(harness.writer.next_ticket, next_ticket);
        assert_eq!(harness.writer.next_data_offset, next_offset);
        assert_eq!(
            harness.writer.resource_ledger.snapshot(),
            expected_resource_charge
        );
        assert_eq!(
            harness.writer.seal_index.len(),
            usize::try_from(next_ticket)?
        );
        assert_eq!(
            harness.history.published_tickets,
            (0..next_ticket).collect::<Vec<_>>()
        );
        assert_eq!(
            harness.history.acknowledged_tickets,
            prior_acknowledged_tickets
        );

        let syncs_before = harness.disk.file(harness.writer.inode)?.successful_syncs;
        harness.commit(b"after recovery", last_commit_ts + 10)?;
        assert_eq!(harness.writer.next_ticket, next_ticket + 1);
        assert_eq!(
            harness.disk.file(harness.writer.inode)?.successful_syncs,
            syncs_before + 1
        );
        let mut expected_acknowledged_tickets = prior_acknowledged_tickets.to_vec();
        expected_acknowledged_tickets.push(next_ticket);
        assert_eq!(
            harness.history.acknowledged_tickets,
            expected_acknowledged_tickets
        );
        assert_eq!(
            harness.writer.seal_index.len(),
            usize::try_from(next_ticket + 1)?
        );
        assert_eq!(harness.writer.last_commit_ts, last_commit_ts + 10);

        // Power loss retains the successful DATA+FRONTIER sync and drops any
        // volatile runtime state. Reopen through production discovery,
        // selection, and fresh-inode installation.
        harness.disk.power_loss();
        let crashed_image = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
            .expect("normalized Active image survives power loss")
            .to_vec();
        fs::write(&path, &crashed_image)?;
        let durable_anchor = manifest_boundary(
            harness
                .disk
                .stable_active_anchor()
                .expect("recovery published an Active anchor"),
        );
        let reopened_selection = active_selection(&path, durable_anchor)?;
        assert_eq!(
            reopened_selection.active_boundary.ticket_end,
            next_ticket + 1
        );
        let reopened = install_active_recovery(&path, &reopened_selection)?;
        let reopened_image = fs::read(&path)?;
        assert_eq!(reopened.image_len, u64::try_from(reopened_image.len())?);
        let reopened_image_digest: [u8; 32] = Sha256::digest(&reopened_image).into();
        assert_eq!(reopened.image_digest(), reopened_image_digest);
        let mut all_values = expected_values.to_vec();
        all_values.push(b"after recovery");
        assert_batch_values(&reopened.batches, &all_values);
        assert_eq!(reopened.next_ticket, next_ticket + 1);
        assert_eq!(reopened.append_offset, u64::try_from(reopened_image.len())?);
        assert_eq!(reopened.seal_index, harness.writer.seal_index);
        let logical_digest: [u8; 32] = harness.writer.logical_hash.clone().finalize().into();
        assert_eq!(reopened.active_boundary.prefix_digest, logical_digest);
        let physical_digest: [u8; 32] = harness.writer.physical_hash.clone().finalize().into();
        assert_eq!(reopened.image_digest(), physical_digest);
        assert_eq!(reopened.last_recorded_at, harness.writer.last_recorded_at);
        Ok(())
    }

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
    fn serial_admission_rejection_preserves_ticket_offset_and_wal_bytes() -> Result<()> {
        let mut harness = SerialHarness::create_with_resource_limits(WalV7ResourceLimits {
            max_logical_wal_bytes: u64::MAX,
            max_source_spool_bytes: 8192,
            max_recovery_workspace_bytes: 0,
            max_buffer_memory_bytes: 4096,
            max_seal_index_bytes: 16,
        })?;
        harness.commit(b"first", 10)?;

        let expected_charge = WalV7ResourceCharge {
            logical_wal_bytes: 8192,
            source_spool_bytes: 8192,
            recovery_workspace_bytes: 0,
            buffer_memory_bytes: 0,
            seal_index_bytes: 8,
        };
        assert_eq!(harness.writer.resource_ledger.snapshot(), expected_charge);

        let ticket_before = harness.writer.next_ticket;
        let offset_before = harness.writer.next_data_offset;
        let wal_before = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
            .expect("serial Active WAL exists")
            .to_vec();
        let history_before = (
            harness.history.published_tickets.clone(),
            harness.history.acknowledged_tickets.clone(),
        );

        let error = harness
            .begin_commit(b"again", 20)
            .expect_err("the second batch exceeds the source-spool limit");
        assert_eq!(
            error.downcast_ref::<WalV7ReservationError>(),
            Some(&WalV7ReservationError::SourceSpoolLimit)
        );
        assert_eq!(harness.writer.phase, CommitPhase::Idle);
        assert_eq!(harness.writer.next_ticket, ticket_before);
        assert_eq!(harness.writer.next_data_offset, offset_before);
        assert_eq!(
            harness
                .disk
                .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
                .expect("serial Active WAL exists after rejection"),
            wal_before
        );
        assert_eq!(
            (
                harness.history.published_tickets.clone(),
                harness.history.acknowledged_tickets.clone(),
            ),
            history_before
        );
        assert_eq!(harness.writer.resource_ledger.snapshot(), expected_charge);
        Ok(())
    }

    #[test]
    fn serial_commits_reuse_one_frame_buffer_capacity() -> Result<()> {
        let mut harness = SerialHarness::create_with_resource_limits(WalV7ResourceLimits {
            max_buffer_memory_bytes: WAL_V7_FRAME_LEN as u64,
            ..model_resource_limits()
        })?;
        for ticket in 0..3 {
            let pending = harness.begin_commit(b"one frame", (ticket + 1) * 10)?;
            assert_eq!(
                harness
                    .writer
                    .resource_ledger
                    .snapshot()
                    .buffer_memory_bytes,
                WAL_V7_FRAME_LEN as u64
            );
            harness.finish_commit(pending)?;
            assert_eq!(
                harness.writer.resource_ledger.snapshot(),
                WalV7ResourceCharge {
                    logical_wal_bytes: (ticket + 1) * 2 * WAL_V7_FRAME_LEN as u64,
                    source_spool_bytes: (ticket + 1) * 2 * WAL_V7_FRAME_LEN as u64,
                    recovery_workspace_bytes: 0,
                    buffer_memory_bytes: 0,
                    seal_index_bytes: (ticket + 1) * std::mem::size_of::<u64>() as u64,
                }
            );
            assert_eq!(harness.writer.next_ticket, ticket + 1);
        }
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
            let charged_after_admission = harness.writer.resource_ledger.snapshot();
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
            assert_eq!(
                harness.writer.resource_ledger.snapshot(),
                charged_after_admission
            );
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
    fn empty_recovery_normalizes_twice_then_appends_and_reopens() -> Result<()> {
        let (image, anchor) = empty_image_with_torn_tail()?;
        normalize_twice_continue_and_reopen(image, anchor, &[], &[])
    }

    #[test]
    fn recovery_of_fragmented_batches_needs_no_outstanding_data_buffers() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        let value = vec![b'x'; WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY + 1];
        harness.commit(&value, 10)?;
        harness.commit(b"small", 20)?;
        let image = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("acknowledged Active WAL exists")
            .to_vec();
        let anchor = manifest_boundary(
            harness
                .disk
                .stable_active_anchor()
                .expect("creation persisted the Active anchor"),
        );
        // The recovery helper reopens with only one frame of buffer capacity,
        // which must not be consumed by the larger persisted batch.
        normalize_twice_continue_and_reopen(image, anchor, &[&value, b"small"], &[0, 1])
    }

    #[test]
    fn recovery_charges_actual_coalesced_frames_at_capacity() -> Result<()> {
        let (image, anchor) = coalesced_group_image()?;
        let recovered_wal_bytes = 3 * WAL_V7_FRAME_LEN as u64;
        let after_append_wal_bytes = recovered_wal_bytes + 2 * WAL_V7_FRAME_LEN as u64;
        for (wal_limit, spool_limit, expected_error) in [
            (
                recovered_wal_bytes,
                u64::MAX,
                Some(WalV7ReservationError::LogicalWalLimit),
            ),
            (
                u64::MAX,
                recovered_wal_bytes,
                Some(WalV7ReservationError::SourceSpoolLimit),
            ),
            (after_append_wal_bytes, after_append_wal_bytes, None),
        ] {
            let directory = TestDirectory::new()?;
            let path = directory.0.join("active.wal");
            fs::write(&path, &image)?;
            let selection = active_selection(&path, anchor)?;
            let installed = install_active_recovery(&path, &selection)?;
            let normalized_image = fs::read(&path)?;
            // Two DATA frames share one nonzero FRONTIER. The immutable
            // header and generation-zero frame belong to startup headroom.
            assert_eq!(normalized_image.len(), 5 * WAL_V7_FRAME_LEN);
            let mut harness = SerialHarness::from_installed_recovery(
                &normalized_image,
                installed,
                vec![0, 1],
                WalV7ResourceLimits {
                    max_logical_wal_bytes: wal_limit,
                    max_source_spool_bytes: spool_limit,
                    max_buffer_memory_bytes: WAL_V7_FRAME_LEN as u64,
                    ..model_resource_limits()
                },
            )?;
            let recovered_charge = WalV7ResourceCharge {
                logical_wal_bytes: recovered_wal_bytes,
                source_spool_bytes: recovered_wal_bytes,
                seal_index_bytes: 2 * std::mem::size_of::<u64>() as u64,
                ..WalV7ResourceCharge::default()
            };
            assert_eq!(harness.writer.resource_ledger.snapshot(), recovered_charge);
            assert_eq!(harness.writer.next_ticket, 2);
            assert_eq!(harness.writer.next_data_offset, 5 * WAL_V7_FRAME_LEN as u64);

            if let Some(expected_error) = expected_error {
                let error = harness
                    .begin_commit(b"after recovery", 30)
                    .expect_err("the recovered prefix fills the configured cap");
                assert_eq!(
                    error.downcast_ref::<WalV7ReservationError>(),
                    Some(&expected_error)
                );
                assert_eq!(harness.writer.resource_ledger.snapshot(), recovered_charge);
                assert_eq!(harness.writer.next_ticket, 2);
                assert_eq!(harness.writer.next_data_offset, 5 * WAL_V7_FRAME_LEN as u64);
                assert_eq!(
                    harness.disk.path_bytes(ACTIVE_WAL_PATH, ImageView::Cached),
                    Some(normalized_image.as_slice())
                );
            } else {
                harness.commit(b"after recovery", 30)?;
                assert_eq!(
                    harness.writer.resource_ledger.snapshot(),
                    WalV7ResourceCharge {
                        logical_wal_bytes: after_append_wal_bytes,
                        source_spool_bytes: after_append_wal_bytes,
                        seal_index_bytes: 3 * std::mem::size_of::<u64>() as u64,
                        ..WalV7ResourceCharge::default()
                    }
                );
                assert_eq!(harness.history.acknowledged_tickets, [0, 1, 2]);
            }
        }
        normalize_twice_continue_and_reopen(image, anchor, &[b"first", b"second"], &[0, 1])
    }

    #[test]
    fn recovery_relocates_a_frontier_behind_speculative_data_then_continues() -> Result<()> {
        let (image, anchor) = marker_behind_speculative_data_image()?;
        let directory = TestDirectory::new()?;
        let path = directory.0.join("candidate.wal");
        fs::write(&path, &image)?;
        let selection = active_selection(&path, anchor)?;
        assert!(selection.candidate.frame_offset > selection.active_boundary.durable_end);
        assert_eq!(selection.active_boundary.ticket_end, 1);

        // The fixture proves ticket 0 durable, but carries no evidence that
        // the caller observed its ACK before the process stopped.
        normalize_twice_continue_and_reopen(image, anchor, &[b"durable"], &[])
    }

    #[test]
    fn active_recovery_falls_back_from_a_complete_marker_with_corrupt_data() -> Result<()> {
        let mut harness = SerialHarness::create()?;
        harness.commit(b"durable", 10)?;
        let pending = harness.begin_commit(b"unacknowledged", 20)?;
        let data_offset = pending.data_offsets[0];
        let mut corrupt_data: [u8; WAL_V7_FRAME_LEN] = frame_at(
            harness
                .disk
                .path_bytes(ACTIVE_WAL_PATH, ImageView::Cached)
                .expect("cached Active WAL exists"),
            data_offset,
        )?
        .try_into()?;
        corrupt_data[WAL_V7_FRAME_HEADER_LEN] ^= 0x80;
        harness.disk.write_at(
            harness.writer.inode,
            usize::try_from(data_offset)?,
            &corrupt_data,
        )?;

        let data_start = usize::try_from(data_offset)?;
        let frontier_start = usize::try_from(pending.frontier_offset)?;
        harness.interrupt_sync(&[
            data_start..data_start + WAL_V7_FRAME_LEN,
            frontier_start..frontier_start + WAL_V7_FRAME_LEN,
        ])?;
        assert_eq!(harness.history.acknowledged_tickets, [0]);
        let image = harness
            .disk
            .path_bytes(ACTIVE_WAL_PATH, ImageView::Stable)
            .expect("uncertain sync persisted its selected ranges")
            .to_vec();
        let anchor = manifest_boundary(
            harness
                .disk
                .stable_active_anchor()
                .expect("creation persisted the original empty Active anchor"),
        );
        let directory = TestDirectory::new()?;
        let path = directory.0.join("candidate.wal");
        fs::write(&path, &image)?;
        let selection = active_selection(&path, anchor)?;
        assert_eq!(selection.active_boundary.ticket_end, 1);
        assert!(selection.diagnostics.fallback_reason.is_some());

        normalize_twice_continue_and_reopen(image, anchor, &[b"durable"], &[0])
    }

    #[test]
    fn trailing_control_prefix_normalizes_and_continues_after_reopen() -> Result<()> {
        let (image, anchor) = trailing_control_image()?;
        let directory = TestDirectory::new()?;
        let path = directory.0.join("candidate.wal");
        fs::write(&path, &image)?;
        let selection = active_selection(&path, anchor)?;
        assert_eq!(selection.active_boundary.ticket_end, 2);
        assert_eq!(
            selection.candidate.frontier.durable_end,
            selection.candidate.frame_offset
        );
        assert_eq!(
            selection.candidate.frontier.previous_frontier_offset,
            4 * WAL_V7_FRAME_LEN as u64
        );

        normalize_twice_continue_and_reopen(image, anchor, &[b"first", b"second"], &[0, 1])
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
