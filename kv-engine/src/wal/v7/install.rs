//! Crash-safe fresh-inode installation for an Active v7 recovery boundary.
//!
//! The caller must hold exclusive recovery ownership: all descriptors and
//! workers for the previous WAL image must be drained, and no writer may
//! mutate the source between candidate selection and installation.

use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use super::{
    codec::{
        WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WAL_V7_LOGICAL_BATCH_HEADER_LEN,
        WalV7DataFragmentHeader, WalV7FrameKind, WalV7Frontier, WalV7Header,
        WalV7LogicalBatchHeader, decode_frame_structural, decode_frontier_successor,
        encode_frontier_successor, encode_generation_zero_frontier, frame_record_digest,
    },
    recovery::{
        WalV7RecoveryAuthority, WalV7RecoveryKind, WalV7RecoverySelection,
        discover_frontier_candidates, select_recovery_candidate,
    },
};
use crate::{
    pitr::{
        LIVE_WAL_V5_LIMITS, RecordedAt, WalBatch, decode_wal_entry_stream, manifest::ActiveBoundary,
    },
    wal::MAX_WAL_FILE_SIZE,
};

const FRAME_LEN_U64: u64 = WAL_V7_FRAME_LEN as u64;
const HEADER_LEN_U64: u64 = WAL_V7_HEADER_LEN as u64;
const COPY_BUFFER_LEN: usize = 64 * 1024;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Installed Active boundary and the state required to resume v7 appends.
///
/// `logical_hasher` and `physical_hasher` are live hash states positioned at
/// the accepted logical prefix and the normalized physical image respectively.
pub(crate) struct WalV7InstalledRecovery {
    pub(crate) active_boundary: ActiveBoundary,
    pub(crate) frontier: WalV7Frontier,
    pub(crate) frontier_offset: u64,
    pub(crate) frontier_digest: [u8; 32],
    pub(crate) image_len: u64,
    pub(crate) append_offset: u64,
    pub(crate) next_ticket: u64,
    pub(crate) last_recorded_at: Option<RecordedAt>,
    pub(crate) batches: Vec<WalBatch>,
    pub(crate) seal_index: Vec<u64>,
    pub(crate) logical_hasher: Sha256,
    pub(crate) physical_hasher: Sha256,
}

/// Active recovery result after authoritative selection and durable installation.
pub(crate) struct WalV7ActiveRecovery {
    pub(crate) selection: WalV7RecoverySelection,
    pub(crate) installed: WalV7InstalledRecovery,
}

impl WalV7InstalledRecovery {
    pub(crate) fn image_digest(&self) -> [u8; 32] {
        self.physical_hasher.clone().finalize().into()
    }
}

/// Discover, select, and install an Active v7 WAL using caller-supplied
/// manifest authority.
///
/// The caller must hold exclusive recovery ownership from before reading the
/// authority through successful installation. This function never derives
/// durable anchors or chain authority from the WAL file itself. It returns
/// only after fresh-inode installation and its directory sync have completed.
///
/// # Errors
/// Returns an error if the supplied authority is not Active, the header or
/// candidate prefixes fail validation, selection cannot satisfy every durable
/// anchor, or fresh-inode installation fails.
pub(crate) fn recover_and_install_active(
    path: impl AsRef<Path>,
    authority: WalV7RecoveryAuthority,
) -> Result<WalV7ActiveRecovery> {
    let WalV7RecoveryAuthority::Active {
        predecessor,
        durable_anchors,
    } = authority
    else {
        anyhow::bail!("immutable v7 recovery images cannot use Active installation")
    };

    let path = path.as_ref();
    let path_metadata = fs::symlink_metadata(path).context("inspect Active v7 WAL path")?;
    ensure!(
        path_metadata.file_type().is_file(),
        "Active v7 WAL path is not a regular file"
    );

    let mut source = File::open(path).context("open Active v7 WAL for recovery")?;
    let file_len = source
        .metadata()
        .context("read Active v7 WAL metadata")?
        .len();
    let mut header_bytes = [0_u8; WAL_V7_HEADER_LEN];
    source
        .seek(SeekFrom::Start(0))
        .context("seek Active v7 WAL header")?;
    source
        .read_exact(&mut header_bytes)
        .context("read Active v7 WAL header")?;
    let header = WalV7Header::decode(&header_bytes)?;
    ensure!(
        header.encode()? == header_bytes,
        "Active v7 WAL header is not canonical"
    );
    let header_digest = header.digest()?;
    let discovery = discover_frontier_candidates(&mut source, file_len, &header_digest)?;
    let selection = select_recovery_candidate(
        &mut source,
        file_len,
        &discovery,
        WalV7RecoveryAuthority::Active {
            predecessor,
            durable_anchors,
        },
    )?;
    drop(source);

    let installed = install_active_recovery(path, &selection)?;

    Ok(WalV7ActiveRecovery {
        selection,
        installed,
    })
}

#[derive(Clone, Copy)]
struct RetainedFrontier {
    offset: u64,
    frontier: WalV7Frontier,
    digest: [u8; 32],
}

struct BatchAssembly {
    ticket: u64,
    fragment_count: u32,
    next_fragment: u32,
    batch_len: usize,
    first_data_offset: u64,
    bytes: Vec<u8>,
}

struct RebuiltPrefix {
    last_frontier: RetainedFrontier,
    next_ticket: u64,
    last_commit_ts: u64,
    last_recorded_at: Option<RecordedAt>,
    batches: Vec<WalBatch>,
    seal_index: Vec<u64>,
    logical_hasher: Sha256,
    physical_hasher: Sha256,
}

/// Normalize an Active v7 WAL into a fresh inode and durably install it.
///
/// The selected candidate is revalidated against the source header and retained
/// frontier chain while its covered prefix is copied. Speculative bytes and the
/// old candidate frame are omitted. A replacement certificate is appended at
/// `durable_end`; the empty boundary retains its canonical generation-zero
/// frame without adding another marker.
///
/// # Errors
/// Returns an error on any validation, read, write, file-sync, rename, or
/// directory-sync failure. A failure after rename still fails recovery; callers
/// must not publish or serve state from this invocation.
pub(crate) fn install_active_recovery(
    path: impl AsRef<Path>,
    selection: &WalV7RecoverySelection,
) -> Result<WalV7InstalledRecovery> {
    ensure!(
        selection.kind == WalV7RecoveryKind::Active,
        "immutable v7 recovery images cannot be normalized"
    );

    let path = path.as_ref();
    let file_name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .context("Active v7 WAL path has no file name")?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let source_path = path.to_path_buf();
    let path_metadata = fs::symlink_metadata(path).context("inspect Active v7 WAL path")?;
    ensure!(
        path_metadata.file_type().is_file(),
        "Active v7 WAL path is not a regular file"
    );
    let mut source = File::open(path).context("open Active v7 WAL for normalization")?;
    let source_metadata = source.metadata().context("read Active v7 WAL metadata")?;
    ensure!(
        source_metadata.is_file(),
        "Active v7 WAL is not a regular file"
    );
    let source_len = source_metadata.len();
    ensure!(
        (HEADER_LEN_U64 + FRAME_LEN_U64..=MAX_WAL_FILE_SIZE).contains(&source_len),
        "Active v7 WAL length is outside the supported range"
    );

    let mut header_bytes = [0_u8; WAL_V7_HEADER_LEN];
    source
        .seek(SeekFrom::Start(0))
        .context("seek Active v7 WAL header")?;
    source
        .read_exact(&mut header_bytes)
        .context("read Active v7 WAL header")?;
    let header = WalV7Header::decode(&header_bytes)?;
    ensure!(
        header.encode()? == header_bytes,
        "Active v7 WAL header is not canonical"
    );
    validate_boundary_identity(selection.active_boundary, header)?;
    validate_selected_frontier(selection)?;
    let header_digest: [u8; 32] = Sha256::digest(header_bytes).into();

    let parent_dir = File::open(parent).context("open Active v7 WAL directory")?;
    let temp_prefix = recovery_temp_prefix(file_name);
    cleanup_stale_recovery_images(parent, &parent_dir, &temp_prefix)?;
    let temp_path = parent.join(format!(
        "{temp_prefix}{}-{}",
        std::process::id(),
        TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));

    let mut temp_created = false;
    let result = (|| -> Result<WalV7InstalledRecovery> {
        let mut temp = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .context("create Active v7 recovery image")?;
        temp_created = true;
        fs::set_permissions(&temp_path, source_metadata.permissions())
            .context("preserve Active v7 WAL permissions")?;

        let mut rebuilt = copy_and_rebuild_prefix(
            &mut source,
            &mut temp,
            source_len,
            header_bytes,
            header_digest,
            selection,
        )?;

        let (frontier, frontier_offset, frontier_digest, image_len) =
            if selection.active_boundary.ticket_end == 0 {
                let frame = encode_generation_zero_frontier(header_digest)?;
                let digest = frame_record_digest(&frame)?;
                ensure!(
                    rebuilt.last_frontier.offset == HEADER_LEN_U64
                        && rebuilt.last_frontier.frontier == selection.candidate.frontier
                        && rebuilt.last_frontier.digest == digest,
                    "empty Active recovery did not retain canonical generation zero"
                );
                ensure!(
                    selection.candidate.frame_digest == digest,
                    "selected empty Active FRONTIER digest changed after verification"
                );
                (
                    rebuilt.last_frontier.frontier,
                    HEADER_LEN_U64,
                    digest,
                    HEADER_LEN_U64 + FRAME_LEN_U64,
                )
            } else {
                let frontier_offset = selection.active_boundary.durable_end;
                let frontier = selection.candidate.frontier;
                let frame = encode_frontier_successor(
                    frontier,
                    rebuilt.last_frontier.frontier,
                    rebuilt.last_frontier.offset,
                    &rebuilt.last_frontier.digest,
                    frontier_offset,
                    header_digest,
                )?;
                validate_selected_candidate_frame(
                    &mut source,
                    source_len,
                    selection,
                    header_digest,
                    rebuilt.last_frontier,
                )?;
                temp.write_all(&frame)
                    .context("append normalized Active v7 recovery frontier")?;
                rebuilt.physical_hasher.update(frame);
                let image_len = frontier_offset
                    .checked_add(FRAME_LEN_U64)
                    .context("normalized Active v7 image length overflows")?;
                ensure!(
                    image_len <= MAX_WAL_FILE_SIZE,
                    "normalized Active v7 image exceeds the 1 GiB limit"
                );
                (
                    frontier,
                    frontier_offset,
                    frame_record_digest(&frame)?,
                    image_len,
                )
            };

        ensure!(
            temp.metadata()?.len() == image_len,
            "normalized Active v7 WAL has an unexpected length"
        );
        ensure!(
            source.metadata()?.len() == source_len,
            "Active v7 WAL changed during exclusive recovery"
        );
        temp.sync_all()
            .context("sync normalized Active v7 recovery image")?;
        drop(temp);

        fs::rename(&temp_path, &source_path)
            .context("atomically replace Active v7 WAL with normalized image")?;
        parent_dir
            .sync_all()
            .context("sync Active v7 WAL directory after replacement")?;

        Ok(WalV7InstalledRecovery {
            active_boundary: selection.active_boundary,
            frontier,
            frontier_offset,
            frontier_digest,
            image_len,
            append_offset: image_len,
            next_ticket: rebuilt.next_ticket,
            last_recorded_at: rebuilt.last_recorded_at,
            batches: rebuilt.batches,
            seal_index: rebuilt.seal_index,
            logical_hasher: rebuilt.logical_hasher,
            physical_hasher: rebuilt.physical_hasher,
        })
    })();

    match result {
        Err(operation_error) if temp_created => {
            match cleanup_failed_recovery_image(&temp_path, &parent_dir) {
                Ok(()) => Err(operation_error),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "{operation_error:#}; additionally, failed to durably clean the temporary Active v7 recovery image: {cleanup_error:#}"
                )),
            }
        }
        other => other,
    }
}

fn recovery_temp_prefix(file_name: &OsStr) -> String {
    let digest = Sha256::digest(file_name.as_encoded_bytes());
    let mut encoded_digest = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded_digest.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded_digest.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    format!(".toy-kv-v7-recovery-{encoded_digest}.tmp-")
}

/// Remove same-WAL recovery images left by a process that crashed before
/// replacement. Callers must already hold exclusive recovery ownership and a
/// selection produced by validating the canonical WAL. Sync the containing
/// directory before allocating the replacement image so reclaimed space is
/// durable before it is reused.
fn cleanup_stale_recovery_images(parent: &Path, parent_dir: &File, prefix: &str) -> Result<()> {
    for entry in fs::read_dir(parent).context("scan for stale Active v7 recovery images")? {
        let entry = entry.context("read Active v7 recovery directory entry")?;
        if !is_recovery_temp_name(&entry.file_name(), prefix) {
            continue;
        }
        let file_type = entry
            .file_type()
            .context("inspect stale Active v7 recovery image")?;
        if !file_type.is_file() {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("remove stale Active v7 recovery image"),
        }
    }
    // A prior attempt may have unlinked every image before its directory sync
    // failed. An empty scan does not establish durable absence on retry.
    parent_dir
        .sync_all()
        .context("sync directory after checking stale Active v7 recovery images")
}

fn cleanup_failed_recovery_image(temp_path: &Path, parent_dir: &File) -> Result<()> {
    match fs::remove_file(temp_path) {
        Ok(()) => parent_dir
            .sync_all()
            .context("sync directory after removing failed Active v7 recovery image"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => parent_dir
            .sync_all()
            .context("sync directory after confirming failed Active v7 recovery image is absent"),
        Err(error) => Err(error).context("remove failed Active v7 recovery image"),
    }
}

fn is_recovery_temp_name(name: &OsStr, prefix: &str) -> bool {
    let Some(suffix) = name.as_encoded_bytes().strip_prefix(prefix.as_bytes()) else {
        return false;
    };
    let Some(separator) = suffix.iter().position(|byte| *byte == b'-') else {
        return false;
    };
    let (pid, sequence_with_separator) = suffix.split_at(separator);
    let sequence = &sequence_with_separator[1..];
    !pid.is_empty()
        && pid.iter().all(u8::is_ascii_digit)
        && !sequence.is_empty()
        && sequence.iter().all(u8::is_ascii_digit)
}

fn validate_boundary_identity(boundary: ActiveBoundary, header: WalV7Header) -> Result<()> {
    boundary.validate()?;
    ensure!(
        boundary.timeline_id == header.timeline_id.0
            && boundary.archive_epoch_id == header.archive_epoch_id.0
            && boundary.segment_id == header.segment_id.0
            && boundary.incarnation == header.incarnation,
        "Active v7 recovery boundary identifies a different WAL"
    );
    Ok(())
}

fn validate_selected_frontier(selection: &WalV7RecoverySelection) -> Result<()> {
    let boundary = selection.active_boundary;
    let frontier = selection.candidate.frontier;
    ensure!(
        frontier.ticket_end == boundary.ticket_end
            && frontier.durable_end == boundary.durable_end
            && frontier.last_commit_ts == boundary.last_commit_ts
            && frontier.prefix_digest == boundary.prefix_digest,
        "selected v7 FRONTIER differs from its Active recovery boundary"
    );
    if boundary.ticket_end == 0 {
        ensure!(
            selection.candidate.frame_offset == HEADER_LEN_U64,
            "empty Active recovery must select generation zero"
        );
    } else {
        ensure!(
            frontier.generation > 0
                && frontier.durable_end >= HEADER_LEN_U64 + 2 * FRAME_LEN_U64
                && frontier.durable_end <= selection.candidate.frame_offset,
            "selected Active v7 FRONTIER has invalid physical bounds"
        );
    }
    Ok(())
}

fn copy_and_rebuild_prefix(
    source: &mut File,
    destination: &mut File,
    source_len: u64,
    header_bytes: [u8; WAL_V7_HEADER_LEN],
    header_digest: [u8; 32],
    selection: &WalV7RecoverySelection,
) -> Result<RebuiltPrefix> {
    let boundary = selection.active_boundary;
    let prefix_end = if boundary.ticket_end == 0 {
        HEADER_LEN_U64 + FRAME_LEN_U64
    } else {
        boundary.durable_end
    };
    ensure!(
        prefix_end <= source_len && prefix_end <= MAX_WAL_FILE_SIZE,
        "Active v7 retained prefix exceeds the source image"
    );
    source
        .seek(SeekFrom::Start(HEADER_LEN_U64))
        .context("seek Active v7 retained prefix")?;

    destination
        .write_all(&header_bytes)
        .context("copy Active v7 immutable header")?;
    let mut physical_hasher = Sha256::new();
    physical_hasher.update(header_bytes);
    let mut logical_hasher = Sha256::new();
    logical_hasher.update(header_bytes);

    let mut prefix_state = PrefixRebuilder::new(header_digest, logical_hasher)?;
    let mut copied = HEADER_LEN_U64;
    let mut buffer = vec![0_u8; COPY_BUFFER_LEN];
    while copied < prefix_end {
        let remaining = usize::try_from(prefix_end - copied)?;
        let count = remaining.min(buffer.len());
        ensure!(
            count.is_multiple_of(WAL_V7_FRAME_LEN),
            "Active v7 retained prefix is not frame aligned"
        );
        source
            .read_exact(&mut buffer[..count])
            .with_context(|| format!("read Active v7 prefix at {copied}"))?;
        destination
            .write_all(&buffer[..count])
            .with_context(|| format!("copy Active v7 prefix at {copied}"))?;
        physical_hasher.update(&buffer[..count]);

        let (frames, remainder) = buffer[..count].as_chunks::<WAL_V7_FRAME_LEN>();
        ensure!(
            remainder.is_empty(),
            "Active v7 retained prefix has a partial frame"
        );
        for (frame_index, frame) in frames.iter().enumerate() {
            let frame_offset = copied
                .checked_add(
                    u64::try_from(frame_index)?
                        .checked_mul(FRAME_LEN_U64)
                        .context("Active v7 frame offset overflows")?,
                )
                .context("Active v7 frame offset overflows")?;
            prefix_state.accept_frame(frame, frame_offset)?;
        }
        copied = copied
            .checked_add(u64::try_from(count)?)
            .context("Active v7 copied-prefix length overflows")?;
    }

    let mut rebuilt = prefix_state.finish(boundary)?;
    ensure!(
        rebuilt.last_frontier.offset + FRAME_LEN_U64 <= boundary.durable_end
            || boundary.ticket_end == 0,
        "Active v7 retained prefix does not contain the selected predecessor"
    );
    ensure!(
        rebuilt.logical_hasher.clone().finalize().as_slice() == boundary.prefix_digest,
        "rebuilt Active v7 logical digest differs from selected boundary"
    );
    rebuilt.physical_hasher = physical_hasher;

    if boundary.ticket_end > 0 {
        ensure!(
            rebuilt.last_frontier.offset == selection.candidate.frontier.previous_frontier_offset
                && rebuilt.last_frontier.digest
                    == selection.candidate.frontier.previous_frontier_digest
                && rebuilt.last_frontier.frontier.generation.checked_add(1)
                    == Some(selection.candidate.frontier.generation),
            "selected v7 FRONTIER does not extend the retained control chain"
        );
    }

    Ok(rebuilt)
}

struct PrefixRebuilder {
    header_digest: [u8; 32],
    logical_hasher: Sha256,
    last_frontier: Option<RetainedFrontier>,
    assembly: Option<BatchAssembly>,
    next_ticket: u64,
    last_commit_ts: u64,
    last_recorded_at: Option<RecordedAt>,
    batches: Vec<WalBatch>,
    seal_index: Vec<u64>,
}

impl PrefixRebuilder {
    fn new(header_digest: [u8; 32], logical_hasher: Sha256) -> Result<Self> {
        let mut batches = Vec::new();
        batches
            .try_reserve(1)
            .context("reserve recovered v7 batch metadata")?;
        let mut seal_index = Vec::new();
        seal_index
            .try_reserve(1)
            .context("reserve recovered v7 seal index")?;
        Ok(Self {
            header_digest,
            logical_hasher,
            last_frontier: None,
            assembly: None,
            next_ticket: 0,
            last_commit_ts: 0,
            last_recorded_at: None,
            batches,
            seal_index,
        })
    }

    fn accept_frame(&mut self, encoded: &[u8], frame_offset: u64) -> Result<()> {
        ensure!(
            encoded.len() == WAL_V7_FRAME_LEN,
            "invalid frame length in Active v7 retained prefix"
        );
        let decoded = decode_frame_structural(encoded, frame_offset, &self.header_digest)?;
        match decoded.kind {
            WalV7FrameKind::Frontier => self.accept_frontier(encoded, frame_offset),
            WalV7FrameKind::Data => self.accept_data_frame(&decoded.body, frame_offset),
        }?;
        Ok(())
    }

    fn accept_frontier(&mut self, encoded: &[u8], frame_offset: u64) -> Result<()> {
        ensure!(
            self.assembly.is_none(),
            "FRONTIER interrupts a v7 DATA batch"
        );
        let (frontier, digest) = if let Some(previous) = self.last_frontier {
            decode_frontier_successor(
                encoded,
                frame_offset,
                &self.header_digest,
                previous.frontier,
                previous.offset,
                &previous.digest,
            )?
        } else {
            let decoded = decode_frame_structural(encoded, frame_offset, &self.header_digest)?;
            ensure!(
                frame_offset == HEADER_LEN_U64 && decoded.kind == WalV7FrameKind::Frontier,
                "Active v7 retained prefix is missing generation zero"
            );
            let frontier = WalV7Frontier::decode_body(&decoded.body)?;
            frontier.validate_generation_zero(&self.header_digest, frame_offset)?;
            let canonical = encode_generation_zero_frontier(self.header_digest)?;
            ensure!(
                encoded == canonical,
                "Active v7 generation-zero FRONTIER is not canonical"
            );
            (frontier, frame_record_digest(encoded)?)
        };
        self.last_frontier = Some(RetainedFrontier {
            offset: frame_offset,
            frontier,
            digest,
        });
        Ok(())
    }

    fn accept_data_frame(&mut self, body: &[u8], frame_offset: u64) -> Result<()> {
        let (fragment, payload) = WalV7DataFragmentHeader::decode_body(body)?;
        if self.assembly.is_none() {
            ensure!(
                fragment.fragment_index == 0 && fragment.segment_ticket == self.next_ticket,
                "v7 DATA ticket or first fragment index is invalid during installation"
            );
            let batch_len = usize::try_from(fragment.batch_bytes)?;
            let maximum_batch_len = WAL_V7_LOGICAL_BATCH_HEADER_LEN
                .checked_add(LIVE_WAL_V5_LIMITS.max_batch_data_bytes)
                .context("v7 logical batch limit overflows")?;
            ensure!(
                batch_len <= maximum_batch_len,
                "v7 DATA batch exceeds configured installation limit"
            );
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(batch_len)
                .context("reserve v7 batch while rebuilding Active recovery")?;
            bytes.extend_from_slice(payload);
            self.assembly = Some(BatchAssembly {
                ticket: fragment.segment_ticket,
                fragment_count: fragment.fragment_count,
                next_fragment: 1,
                batch_len,
                first_data_offset: frame_offset,
                bytes,
            });
        } else {
            let assembly = self
                .assembly
                .as_mut()
                .context("v7 batch assembly disappeared")?;
            ensure!(
                fragment.segment_ticket == assembly.ticket
                    && fragment.fragment_count == assembly.fragment_count
                    && fragment.batch_bytes == u64::try_from(assembly.batch_len)?
                    && fragment.fragment_index == assembly.next_fragment,
                "v7 DATA fragments disagree during installation"
            );
            assembly.bytes.extend_from_slice(payload);
            assembly.next_fragment = assembly
                .next_fragment
                .checked_add(1)
                .context("v7 DATA fragment index overflows")?;
        }

        let complete = self
            .assembly
            .as_ref()
            .is_some_and(|assembly| assembly.next_fragment == assembly.fragment_count);
        if complete {
            self.finish_batch()?;
        }
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<()> {
        let assembly = self
            .assembly
            .take()
            .context("v7 complete batch has no assembly")?;
        ensure!(
            assembly.bytes.len() == assembly.batch_len
                && assembly.bytes.len() >= WAL_V7_LOGICAL_BATCH_HEADER_LEN,
            "v7 DATA fragments have an invalid total length during installation"
        );
        let data = &assembly.bytes[WAL_V7_LOGICAL_BATCH_HEADER_LEN..];
        let header = WalV7LogicalBatchHeader::decode(
            &assembly.bytes[..WAL_V7_LOGICAL_BATCH_HEADER_LEN],
            data,
            assembly.ticket,
            u64::try_from(assembly.batch_len)?,
            LIVE_WAL_V5_LIMITS,
        )?;
        let recorded_at = RecordedAt {
            secs: header.recorded_at_secs,
            nanos: header.recorded_at_nanos,
        };
        let batch = decode_wal_entry_stream(
            header.commit_ts,
            recorded_at,
            header.entry_count,
            data,
            LIVE_WAL_V5_LIMITS,
        )?;
        ensure!(
            batch.commit_ts > self.last_commit_ts
                && self
                    .last_recorded_at
                    .is_none_or(|previous| batch.recorded_at >= previous),
            "v7 commit timestamps or recorded times are out of order during installation"
        );
        self.logical_hasher.update(&assembly.bytes);
        self.seal_index
            .try_reserve(1)
            .context("grow recovered v7 seal index")?;
        self.seal_index.push(assembly.first_data_offset);
        self.batches
            .try_reserve(1)
            .context("grow recovered v7 batch list")?;
        self.last_commit_ts = batch.commit_ts;
        self.last_recorded_at = Some(batch.recorded_at);
        self.next_ticket = self
            .next_ticket
            .checked_add(1)
            .context("v7 recovered ticket counter overflows")?;
        self.batches.push(batch);
        Ok(())
    }

    fn finish(self, boundary: ActiveBoundary) -> Result<RebuiltPrefix> {
        ensure!(
            self.assembly.is_none(),
            "Active v7 retained prefix ends inside a DATA batch"
        );
        ensure!(
            self.next_ticket == boundary.ticket_end
                && self.last_commit_ts == boundary.last_commit_ts,
            "rebuilt Active v7 ticket or timestamp boundary mismatch"
        );
        let last_frontier = self
            .last_frontier
            .context("Active v7 retained prefix has no generation-zero FRONTIER")?;
        Ok(RebuiltPrefix {
            last_frontier,
            next_ticket: self.next_ticket,
            last_commit_ts: self.last_commit_ts,
            last_recorded_at: self.last_recorded_at,
            batches: self.batches,
            seal_index: self.seal_index,
            logical_hasher: self.logical_hasher,
            physical_hasher: Sha256::new(),
        })
    }
}

fn validate_selected_candidate_frame(
    source: &mut File,
    source_len: u64,
    selection: &WalV7RecoverySelection,
    header_digest: [u8; 32],
    predecessor: RetainedFrontier,
) -> Result<()> {
    let end = selection
        .candidate
        .frame_offset
        .checked_add(FRAME_LEN_U64)
        .context("selected v7 FRONTIER end overflows")?;
    ensure!(
        end <= source_len,
        "selected v7 FRONTIER extends past the source image"
    );
    source
        .seek(SeekFrom::Start(selection.candidate.frame_offset))
        .context("seek selected Active v7 FRONTIER")?;
    let mut encoded = [0_u8; WAL_V7_FRAME_LEN];
    source
        .read_exact(&mut encoded)
        .context("read selected Active v7 FRONTIER")?;
    let (frontier, digest) = decode_frontier_successor(
        &encoded,
        selection.candidate.frame_offset,
        &header_digest,
        predecessor.frontier,
        predecessor.offset,
        &predecessor.digest,
    )?;
    ensure!(
        frontier == selection.candidate.frontier && digest == selection.candidate.frame_digest,
        "selected Active v7 FRONTIER changed after verification"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use anyhow::Result;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{
        pitr::{
            ArchiveEpochId, ChainAnchor, LIVE_WAL_V5_LIMITS, SegmentId, TimelineId, WalEntry,
            encode_v5_batch,
        },
        wal::v7::{
            codec::{
                WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY, WalV7DataFragmentHeader,
                WalV7LogicalBatchHeader, encode_data_frame, encode_generation_zero_frontier,
            },
            recovery::{
                WalV7RecoveryAuthority, discover_frontier_candidates, select_recovery_candidate,
            },
        },
    };

    static TEMP_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Result<Self> {
            let path = std::env::temp_dir().join(format!(
                "toy-kv-v7-install-{}-{}",
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

    fn test_header() -> WalV7Header {
        WalV7Header {
            timeline_id: TimelineId([1; 16]),
            archive_epoch_id: ArchiveEpochId([2; 16]),
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([2; 16]),
            },
            incarnation: [3; 16],
        }
    }

    fn data_frames(
        header_digest: [u8; 32],
        frame_offset: u64,
        ticket: u64,
        batch: &WalBatch,
    ) -> Result<(Vec<[u8; WAL_V7_FRAME_LEN]>, [u8; 32])> {
        let encoded_v5 = encode_v5_batch(batch, LIVE_WAL_V5_LIMITS)?;
        let data_len = usize::try_from(u32::from_be_bytes(encoded_v5[24..28].try_into()?))?;
        let data = &encoded_v5
            [crate::pitr::WAL_V5_BATCH_HEADER_LEN..crate::pitr::WAL_V5_BATCH_HEADER_LEN + data_len];
        let batch_header = WalV7LogicalBatchHeader {
            segment_ticket: ticket,
            commit_ts: batch.commit_ts,
            recorded_at_secs: batch.recorded_at.secs,
            recorded_at_nanos: batch.recorded_at.nanos,
            entry_count: u32::try_from(batch.entries.len())?,
        }
        .encode(data, LIVE_WAL_V5_LIMITS)?;
        let mut logical_batch = Vec::with_capacity(batch_header.len() + data.len());
        logical_batch.extend_from_slice(&batch_header);
        logical_batch.extend_from_slice(data);

        let fragment_count = logical_batch
            .len()
            .div_ceil(WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY);
        let mut frames = Vec::new();
        frames.try_reserve_exact(fragment_count)?;
        for fragment_index in 0..fragment_count {
            let start = fragment_index * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY;
            let end = logical_batch
                .len()
                .min(start + WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY);
            let fragment = WalV7DataFragmentHeader {
                segment_ticket: ticket,
                fragment_index: u32::try_from(fragment_index)?,
                fragment_count: u32::try_from(fragment_count)?,
                batch_bytes: u64::try_from(logical_batch.len())?,
            };
            let body = fragment.encode_body(&logical_batch[start..end])?;
            let offset = frame_offset
                .checked_add(
                    u64::try_from(fragment_index)?
                        .checked_mul(FRAME_LEN_U64)
                        .context("test DATA frame offset overflows")?,
                )
                .context("test DATA frame offset overflows")?;
            frames.push(encode_data_frame(header_digest, offset, &body)?);
        }
        let mut hash = Sha256::new();
        hash.update(test_header().encode()?);
        hash.update(logical_batch);
        Ok((frames, hash.finalize().into()))
    }

    fn active_selection(path: &Path, anchor: ActiveBoundary) -> Result<WalV7RecoverySelection> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let header = WalV7Header::decode(&read_header(&mut file)?)?;
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

    fn read_header(file: &mut File) -> Result<[u8; WAL_V7_HEADER_LEN]> {
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = [0; WAL_V7_HEADER_LEN];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn create_nonempty_image(path: &Path) -> Result<(WalV7Header, ActiveBoundary)> {
        let batch = WalBatch {
            commit_ts: 10,
            recorded_at: RecordedAt {
                secs: 100,
                nanos: 5,
            },
            entries: vec![WalEntry::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            }],
        };
        create_nonempty_image_for_batch(path, batch)
    }

    fn create_nonempty_image_for_batch(
        path: &Path,
        batch: WalBatch,
    ) -> Result<(WalV7Header, ActiveBoundary)> {
        let header = test_header();
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
        let mut image = header_bytes.to_vec();
        let generation_zero = encode_generation_zero_frontier(header_digest)?;
        let generation_zero_digest = frame_record_digest(&generation_zero)?;
        image.extend_from_slice(&generation_zero);

        let data_offset = HEADER_LEN_U64 + FRAME_LEN_U64;
        let (first_data_frames, prefix_digest) =
            data_frames(header_digest, data_offset, 0, &batch)?;
        for frame in &first_data_frames {
            image.extend_from_slice(frame);
        }
        let data_end = data_offset
            .checked_add(
                u64::try_from(first_data_frames.len())?
                    .checked_mul(FRAME_LEN_U64)
                    .context("test DATA prefix length overflows")?,
            )
            .context("test DATA prefix end overflows")?;

        // A speculative DATA slot separates the accepted prefix from the
        // candidate certificate, so installation must relocate the marker.
        let speculative_batch = WalBatch {
            commit_ts: 20,
            recorded_at: RecordedAt {
                secs: 101,
                nanos: 0,
            },
            entries: vec![WalEntry::Put {
                key: b"later".to_vec(),
                value: b"speculative".to_vec(),
            }],
        };
        let (speculative, _) = data_frames(header_digest, data_end, 1, &speculative_batch)?;
        for frame in &speculative {
            image.extend_from_slice(frame);
        }
        let frontier = WalV7Frontier {
            generation: 1,
            ticket_end: 1,
            durable_end: data_end,
            last_commit_ts: batch.commit_ts,
            prefix_digest,
            previous_frontier_offset: HEADER_LEN_U64,
            previous_frontier_digest: generation_zero_digest,
        };
        let marker_offset = data_end
            .checked_add(
                u64::try_from(speculative.len())?
                    .checked_mul(FRAME_LEN_U64)
                    .context("test speculative suffix length overflows")?,
            )
            .context("test frontier offset overflows")?;
        let marker = encode_frontier_successor(
            frontier,
            WalV7Frontier::decode_body(
                &decode_frame_structural(&generation_zero, HEADER_LEN_U64, &header_digest)?.body,
            )?,
            HEADER_LEN_U64,
            &generation_zero_digest,
            marker_offset,
            header_digest,
        )?;
        image.extend_from_slice(&marker);
        fs::write(path, image)?;

        Ok((
            header,
            ActiveBoundary {
                timeline_id: header.timeline_id.0,
                archive_epoch_id: header.archive_epoch_id.0,
                segment_id: header.segment_id.0,
                incarnation: header.incarnation,
                ticket_end: 1,
                durable_end: data_end,
                last_commit_ts: batch.commit_ts,
                prefix_digest,
            },
        ))
    }

    #[test]
    fn active_normalization_relocates_marker_and_is_idempotent() -> Result<()> {
        // Arrange
        let directory = TestDirectory::new()?;
        let path = directory.0.join("active.wal");
        let (_header, anchor) = create_nonempty_image(&path)?;
        let selection = active_selection(&path, anchor)?;
        assert_eq!(selection.candidate.frame_offset, 4 * FRAME_LEN_U64);

        // Act
        let installed = install_active_recovery(&path, &selection)?;

        // Assert
        assert_eq!(installed.active_boundary, anchor);
        assert_eq!(installed.next_ticket, 1);
        assert_eq!(installed.append_offset, 4 * FRAME_LEN_U64);
        assert_eq!(installed.frontier_offset, 3 * FRAME_LEN_U64);
        assert_eq!(installed.batches.len(), 1);
        assert_eq!(installed.seal_index, [2 * FRAME_LEN_U64]);
        assert_eq!(
            installed.last_recorded_at,
            Some(RecordedAt {
                secs: 100,
                nanos: 5
            })
        );
        assert_eq!(fs::metadata(&path)?.len(), installed.image_len);
        let expected_image_digest: [u8; 32] = Sha256::digest(fs::read(&path)?).into();
        assert_eq!(installed.image_digest(), expected_image_digest);

        let normalized = fs::read(&path)?;
        let selected_again = active_selection(&path, anchor)?;
        let second_install = install_active_recovery(&path, &selected_again)?;
        assert_eq!(fs::read(&path)?, normalized);
        assert_eq!(second_install.image_digest(), installed.image_digest());
        Ok(())
    }

    #[test]
    fn active_recovery_coordinator_selects_and_installs_from_manifest_authority() -> Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.0.join("active.wal");
        let (header, anchor) = create_nonempty_image(&path)?;

        let recovered = recover_and_install_active(
            &path,
            WalV7RecoveryAuthority::Active {
                predecessor: header.predecessor,
                durable_anchors: vec![anchor],
            },
        )?;

        assert_eq!(recovered.selection.kind, WalV7RecoveryKind::Active);
        assert_eq!(recovered.selection.active_boundary, anchor);
        assert_eq!(recovered.installed.active_boundary, anchor);
        assert_eq!(recovered.installed.next_ticket, 1);
        assert_eq!(recovered.installed.batches.len(), 1);
        assert_eq!(
            recovered.installed.batches[0].entries,
            [WalEntry::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            }]
        );
        assert_eq!(fs::metadata(&path)?.len(), recovered.installed.image_len);

        Ok(())
    }

    #[test]
    fn active_recovery_coordinator_rejects_wrong_manifest_predecessor_without_mutation()
    -> Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.0.join("active.wal");
        let (header, anchor) = create_nonempty_image(&path)?;
        let original = fs::read(&path)?;
        let wrong_predecessor = ChainAnchor::Genesis {
            archive_epoch_id: ArchiveEpochId([9; 16]),
        };
        assert_ne!(wrong_predecessor, header.predecessor);

        let error = recover_and_install_active(
            &path,
            WalV7RecoveryAuthority::Active {
                predecessor: wrong_predecessor,
                durable_anchors: vec![anchor],
            },
        )
        .err()
        .context("Active recovery accepted the wrong manifest predecessor")?;

        assert!(error.to_string().contains("authoritative segment identity"));
        assert_eq!(fs::read(&path)?, original);

        Ok(())
    }

    #[test]
    fn active_normalization_cleans_only_its_stale_recovery_images() -> Result<()> {
        // Arrange
        let directory = TestDirectory::new()?;
        let path = directory.0.join("active.wal");
        let (_header, anchor) = create_nonempty_image(&path)?;
        let selection = active_selection(&path, anchor)?;
        let prefix = recovery_temp_prefix(path.file_name().context("test WAL has no name")?);
        let stale_image = directory.0.join(format!("{prefix}123-456"));
        fs::write(&stale_image, [0; 4096])?;

        let other_prefix = recovery_temp_prefix(OsStr::new("other.wal"));
        let other_image = directory.0.join(format!("{other_prefix}123-456"));
        fs::write(&other_image, [0; 4096])?;

        // Act
        install_active_recovery(&path, &selection)?;

        // Assert
        assert!(!stale_image.exists());
        assert!(other_image.exists());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stale_recovery_cleanup_requires_sync_on_empty_retry() -> Result<()> {
        let directory = TestDirectory::new()?;
        let prefix = recovery_temp_prefix(OsStr::new("active.wal"));
        let stale_image = directory.0.join(format!("{prefix}123-456"));
        fs::write(&stale_image, b"partial")?;
        // Linux rejects fsync on this descriptor, allowing the unlink to
        // succeed while the required directory barrier fails.
        let unsyncable = File::open("/dev/null")?;

        let first_error =
            cleanup_stale_recovery_images(&directory.0, &unsyncable, &prefix).unwrap_err();
        assert!(!stale_image.exists());
        assert!(first_error.to_string().contains("sync directory"));

        let retry_error =
            cleanup_stale_recovery_images(&directory.0, &unsyncable, &prefix).unwrap_err();
        assert!(retry_error.to_string().contains("sync directory"));

        let parent_dir = File::open(&directory.0)?;
        cleanup_stale_recovery_images(&directory.0, &parent_dir, &prefix)?;
        Ok(())
    }

    #[test]
    fn failed_recovery_image_cleanup_is_idempotent() -> Result<()> {
        let directory = TestDirectory::new()?;
        let parent_dir = File::open(&directory.0)?;
        let temp_path = directory.0.join("partial-recovery-image");
        fs::write(&temp_path, b"partial")?;

        cleanup_failed_recovery_image(&temp_path, &parent_dir)?;
        cleanup_failed_recovery_image(&temp_path, &parent_dir)?;

        assert!(!temp_path.exists());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn active_normalization_supports_non_utf8_paths() -> Result<()> {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};

        // Arrange
        let directory = TestDirectory::new()?;
        let path = directory
            .0
            .join(OsString::from_vec(b"active-\xff.wal".to_vec()));
        let (_header, anchor) = create_nonempty_image(&path)?;
        let selection = active_selection(&path, anchor)?;

        // Act
        let installed = install_active_recovery(&path, &selection)?;

        // Assert
        assert!(path.file_name().is_some_and(|name| name.to_str().is_none()));
        assert_eq!(fs::metadata(&path)?.len(), installed.image_len);
        Ok(())
    }

    #[test]
    fn active_normalization_rebuilds_fragmented_batches() -> Result<()> {
        // Arrange
        let directory = TestDirectory::new()?;
        let path = directory.0.join("active.wal");
        let value = vec![b'x'; 8 * 1024];
        let batch = WalBatch {
            commit_ts: 10,
            recorded_at: RecordedAt {
                secs: 100,
                nanos: 5,
            },
            entries: vec![WalEntry::Put {
                key: b"large".to_vec(),
                value: value.clone(),
            }],
        };
        let (_header, anchor) = create_nonempty_image_for_batch(&path, batch)?;
        let selection = active_selection(&path, anchor)?;

        // Act
        let installed = install_active_recovery(&path, &selection)?;

        // Assert
        assert_eq!(installed.next_ticket, 1);
        assert_eq!(installed.batches.len(), 1);
        assert_eq!(
            installed.batches[0].entries,
            [WalEntry::Put {
                key: b"large".to_vec(),
                value,
            }]
        );
        assert_eq!(installed.seal_index, [2 * FRAME_LEN_U64]);
        let logical_digest: [u8; 32] = installed.logical_hasher.clone().finalize().into();
        assert_eq!(logical_digest, anchor.prefix_digest);
        Ok(())
    }

    #[test]
    fn empty_active_normalization_preserves_generation_zero_only() -> Result<()> {
        // Arrange
        let directory = TestDirectory::new()?;
        let path = directory.0.join("active.wal");
        let header = test_header();
        let header_bytes = header.encode()?;
        let header_digest = header.digest()?;
        let mut image = header_bytes.to_vec();
        image.extend_from_slice(&encode_generation_zero_frontier(header_digest)?);
        image.extend_from_slice(&[0; WAL_V7_FRAME_LEN]);
        fs::write(&path, image)?;
        let anchor = ActiveBoundary {
            timeline_id: header.timeline_id.0,
            archive_epoch_id: header.archive_epoch_id.0,
            segment_id: header.segment_id.0,
            incarnation: header.incarnation,
            ticket_end: 0,
            durable_end: HEADER_LEN_U64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
        };
        let selection = active_selection(&path, anchor)?;

        // Act
        let installed = install_active_recovery(&path, &selection)?;

        // Assert
        assert_eq!(installed.image_len, HEADER_LEN_U64 + FRAME_LEN_U64);
        assert_eq!(installed.frontier_offset, HEADER_LEN_U64);
        assert_eq!(installed.next_ticket, 0);
        assert!(installed.batches.is_empty());
        assert!(installed.seal_index.is_empty());
        assert_eq!(fs::read(&path)?.len(), installed.image_len as usize);
        Ok(())
    }

    #[test]
    fn immutable_selection_cannot_be_normalized() -> Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.0.join("active.wal");
        let (_header, anchor) = create_nonempty_image(&path)?;
        let original = fs::read(&path)?;
        let mut selection = active_selection(&path, anchor)?;
        selection.kind = WalV7RecoveryKind::Immutable;
        let error = install_active_recovery(&path, &selection)
            .err()
            .context("Immutable selection was normalized")?;
        assert!(error.to_string().contains("cannot be normalized"));
        assert_eq!(fs::read(&path)?, original);
        Ok(())
    }
}
