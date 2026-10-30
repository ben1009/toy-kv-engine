//! Crash-safe fresh-inode installation for an Active v8 recovery boundary.
//!
//! The caller must own exclusive recovery: all writers and descriptors for the
//! source image must be drained before selection and installation begin.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use crate::{
    pitr::{RecordedAt, WalV5Limits, manifest::ActiveBoundary},
    wal::MAX_WAL_FILE_SIZE,
};

use super::{
    codec::{
        WAL_V8_FRAME_LEN, WalV8Frontier, WalV8Header, decode_frame_structural,
        encode_frontier_successor, encode_generation_zero_frontier, frame_record_digest,
    },
    recovery::{
        WalV8RecoverySelection, WalV8SealTicket, discover_frontier_candidates,
        replay_selected_batches, select_active_candidate_for_replay,
        select_active_candidate_for_validation,
    },
};

const FRAME_LEN_U64: u64 = WAL_V8_FRAME_LEN as u64;
const HEADER_LEN_U64: u64 = super::WAL_V8_HEADER_LEN as u64;
const COPY_BUFFER_LEN: usize = 64 * 1024;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Recovered v8 Active state after the accepted boundary is normalized and
/// durably installed on a fresh inode.
pub(crate) struct WalV8InstalledRecovery {
    /// The accepted boundary and frontier-chain metadata. Active recovery
    /// leaves `selection.batches` empty and streams replay using the compact
    /// ticket index below.
    pub(crate) selection: WalV8RecoverySelection,
    pub(crate) replay_tickets: Vec<WalV8SealTicket>,
    pub(crate) active_boundary: ActiveBoundary,
    pub(crate) header_digest: [u8; 32],
    pub(crate) frontier: WalV8Frontier,
    pub(crate) frontier_offset: u64,
    pub(crate) frontier_digest: [u8; 32],
    pub(crate) image_len: u64,
    pub(crate) next_ticket: u64,
    pub(crate) installed_dev: u64,
    pub(crate) installed_ino: u64,
    pub(crate) last_recorded_at: Option<RecordedAt>,
}

/// Durably create the canonical empty Active image for a new v8 segment.
///
/// This is a creation path, not recovery. The caller must have a durable
/// manifest transition authorizing the segment identity and predecessor
/// before publishing the returned boundary to writers.
#[cfg(target_os = "linux")]
pub(crate) fn create_empty_active_image(
    path: impl AsRef<Path>,
    header: WalV8Header,
) -> Result<ActiveBoundary> {
    let path = path.as_ref();
    let header_bytes = header.encode()?;
    let physical_header_digest = header.physical_digest()?;
    let initial_prefix_digest = header.initial_prefix_digest()?;
    let generation_zero =
        encode_generation_zero_frontier(physical_header_digest, initial_prefix_digest)?;
    let mut image = [0_u8; super::WAL_V8_HEADER_LEN + WAL_V8_FRAME_LEN];
    image[..super::WAL_V8_HEADER_LEN].copy_from_slice(&header_bytes);
    image[super::WAL_V8_HEADER_LEN..].copy_from_slice(&generation_zero);

    crate::pitr::segment::install_pitr_file_no_replace_with_faults(
        path,
        &image,
        crate::pitr::segment::PitrFileInstallFaults {
            after_header_write_before_remainder: Some(
                "wal_v8.active_image.after_temp_header_write_before_frontier_write",
            ),
            after_file_write_before_sync: Some("wal_v8.active_image.after_temp_write_before_sync"),
            after_file_sync_before_rename: Some(
                "wal_v8.active_image.after_temp_sync_before_rename",
            ),
            after_rename_before_directory_sync: Some(
                "wal_v8.active_image.after_rename_before_directory_sync",
            ),
            inject_empty_active_image_failures: false,
        },
    )?;

    let mut file = File::open(path).context("open newly installed empty v8 Active WAL")?;
    let metadata = file.metadata().context("inspect empty v8 Active WAL")?;
    ensure!(
        metadata.len() == image.len() as u64,
        "new empty v8 Active WAL has an unexpected length"
    );
    let mut installed_image = [0_u8; super::WAL_V8_HEADER_LEN + WAL_V8_FRAME_LEN];
    file.read_exact(&mut installed_image)
        .context("verify newly installed empty v8 Active WAL")?;
    ensure!(
        installed_image == image,
        "new empty v8 Active WAL differs from its canonical image"
    );

    let frame_offset = HEADER_LEN_U64;
    let decoded = decode_frame_structural(
        &generation_zero,
        frame_offset,
        &physical_header_digest,
        &initial_prefix_digest,
    )?;
    ensure!(
        decoded.kind == super::codec::WalV8FrameKind::Frontier,
        "new empty v8 Active WAL does not begin with generation-zero FRONTIER"
    );
    let frontier = WalV8Frontier::decode_body(&decoded.body)?;
    frontier.validate_generation_zero(&initial_prefix_digest, frame_offset)?;

    Ok(ActiveBoundary {
        timeline_id: header.timeline_id.0,
        archive_epoch_id: header.archive_epoch_id.0,
        segment_id: header.segment_id.0,
        incarnation: header.incarnation,
        wal_format: crate::pitr::manifest::PitrWalFormat::V8,
        ticket_end: 0,
        durable_end: frame_offset,
        last_commit_ts: 0,
        prefix_digest: initial_prefix_digest,
    })
}

/// Discover, validate, normalize and install an Active v8 WAL.
///
/// The complete source image remains the recovery authority until the
/// replacement has been synced, renamed over it, and the containing directory
/// has been synced. The selected marker is rewritten at the normalized physical
/// boundary so a second crash during startup still leaves a recovery
/// certificate.
pub(crate) fn recover_and_install_active(
    path: impl AsRef<Path>,
    anchors: &[ActiveBoundary],
    expected_predecessor: crate::pitr::ChainAnchor,
    limits: WalV5Limits,
    max_image_len: u64,
) -> Result<WalV8InstalledRecovery> {
    let path = path.as_ref();
    let path_metadata = fs::symlink_metadata(path).context("inspect Active v8 WAL path")?;
    ensure!(
        path_metadata.file_type().is_file(),
        "Active v8 WAL path is not a regular file"
    );

    let mut source = File::open(path).context("open Active v8 WAL for recovery")?;
    let source_metadata = source.metadata().context("read Active v8 WAL metadata")?;
    ensure!(
        source_metadata.is_file(),
        "Active v8 WAL is not a regular file"
    );
    let source_len = source_metadata.len();
    ensure!(
        (HEADER_LEN_U64 + FRAME_LEN_U64..=MAX_WAL_FILE_SIZE).contains(&source_len),
        "Active v8 WAL length is outside the supported range"
    );

    let mut header_bytes = [0; super::WAL_V8_HEADER_LEN];
    source
        .seek(SeekFrom::Start(0))
        .context("seek Active v8 WAL header")?;
    source
        .read_exact(&mut header_bytes)
        .context("read Active v8 WAL header")?;
    let header = super::codec::WalV8Header::decode(&header_bytes)?;
    ensure!(
        header.encode()? == header_bytes,
        "Active v8 WAL header is not canonical"
    );
    ensure!(
        header.predecessor == expected_predecessor,
        "v8 WAL predecessor differs from authoritative segment identity"
    );
    let header_digest: [u8; 32] = Sha256::digest(header_bytes).into();
    let initial_prefix_digest = header.initial_prefix_digest()?;
    let discovery = discover_frontier_candidates(
        &mut source,
        source_len,
        &header_digest,
        &initial_prefix_digest,
    )?;
    let (selection, replay_tickets) = select_active_candidate_for_replay(
        &mut source,
        source_len,
        &discovery,
        anchors,
        expected_predecessor,
        limits,
    )?;
    let normalized_len = normalized_image_len(&selection)?;
    ensure!(
        normalized_len <= max_image_len && normalized_len <= MAX_WAL_FILE_SIZE,
        "normalized Active v8 WAL exceeds the configured segment limit"
    );

    let installed = install_normalized_image(InstallRequest {
        path,
        source: &mut source,
        source_len,
        source_permissions: source_metadata.permissions(),
        selection,
        replay_tickets,
        header_digest,
        initial_prefix_digest,
        limits,
        normalized_len,
    })?;
    Ok(installed)
}

fn normalized_image_len(selection: &WalV8RecoverySelection) -> Result<u64> {
    if selection.active_boundary.ticket_end == 0 {
        return Ok(HEADER_LEN_U64 + FRAME_LEN_U64);
    }
    selection
        .active_boundary
        .durable_end
        .checked_add(FRAME_LEN_U64)
        .context("normalized Active v8 image length overflows")
}

struct InstallRequest<'a> {
    path: &'a Path,
    source: &'a mut File,
    source_len: u64,
    source_permissions: fs::Permissions,
    selection: WalV8RecoverySelection,
    replay_tickets: Vec<WalV8SealTicket>,
    header_digest: [u8; 32],
    initial_prefix_digest: super::digest::WalV8Digest,
    limits: WalV5Limits,
    normalized_len: u64,
}

fn install_normalized_image(request: InstallRequest<'_>) -> Result<WalV8InstalledRecovery> {
    let InstallRequest {
        path,
        source,
        source_len,
        source_permissions,
        selection,
        replay_tickets,
        header_digest,
        initial_prefix_digest,
        limits,
        normalized_len,
    } = request;
    let file_name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .context("Active v8 WAL path has no file name")?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_dir = File::open(parent).context("open Active v8 WAL directory")?;
    let temp_prefix = recovery_temp_prefix(file_name);
    cleanup_stale_recovery_images(parent, &parent_dir, &temp_prefix)?;
    let temp_path = parent.join(format!(
        "{temp_prefix}{}-{}",
        std::process::id(),
        TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));

    let mut temp_created = false;
    let result = (|| -> Result<WalV8InstalledRecovery> {
        let mut temp = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .context("create Active v8 recovery image")?;
        temp_created = true;
        fs::set_permissions(&temp_path, source_permissions)
            .context("preserve Active v8 WAL permissions")?;

        let (frontier, frontier_offset, frontier_digest) =
            if selection.active_boundary.ticket_end == 0 {
                ensure!(
                    selection.candidate.frontier.generation == 0
                        && selection.candidate.frame_offset == HEADER_LEN_U64
                        && selection.candidate.frontier.prefix_digest == initial_prefix_digest,
                    "empty Active v8 recovery selected a noncanonical initial frontier"
                );
                let frame = encode_generation_zero_frontier(header_digest, initial_prefix_digest)?;
                let digest = frame_record_digest(&frame)?;
                ensure!(
                    selection.candidate.frame_digest == digest,
                    "empty Active v8 FRONTIER digest changed after selection"
                );
                validate_selected_candidate(
                    source,
                    source_len,
                    &selection,
                    header_digest,
                    initial_prefix_digest,
                )?;
                temp.write_all(&selection.header_bytes)
                    .context("write normalized Active v8 header")?;
                temp.write_all(&frame)
                    .context("write normalized Active v8 generation-zero FRONTIER")?;
                (selection.candidate.frontier, HEADER_LEN_U64, digest)
            } else {
                let durable_end = selection.active_boundary.durable_end;
                ensure!(
                    durable_end >= HEADER_LEN_U64 + FRAME_LEN_U64
                        && durable_end.is_multiple_of(FRAME_LEN_U64)
                        && durable_end <= source_len,
                    "Active v8 durable boundary is outside the source image"
                );
                temp.write_all(&selection.header_bytes)
                    .context("write normalized Active v8 header")?;
                copy_retained_prefix(source, &mut temp, durable_end - HEADER_LEN_U64)?;
                ensure!(
                    source.metadata()?.len() == source_len,
                    "Active v8 WAL changed during exclusive recovery"
                );

                let frontier_offset = durable_end;
                let generation = selection
                    .last_frontier
                    .generation
                    .checked_add(1)
                    .context("normalized Active v8 FRONTIER generation overflows")?;
                let frontier = WalV8Frontier {
                    generation,
                    ticket_end: selection.active_boundary.ticket_end,
                    durable_end,
                    last_commit_ts: selection.active_boundary.last_commit_ts,
                    prefix_digest: selection.active_boundary.prefix_digest,
                    previous_frontier_offset: selection.last_frontier_offset,
                    previous_frontier_digest: selection.last_frontier_digest,
                };
                let frame = encode_frontier_successor(
                    frontier,
                    selection.last_frontier,
                    selection.last_frontier_offset,
                    &selection.last_frontier_digest,
                    frontier_offset,
                    header_digest,
                    initial_prefix_digest,
                )?;
                validate_selected_candidate(
                    source,
                    source_len,
                    &selection,
                    header_digest,
                    initial_prefix_digest,
                )?;
                temp.write_all(&frame)
                    .context("append normalized Active v8 recovery FRONTIER")?;
                (frontier, frontier_offset, frame_record_digest(&frame)?)
            };

        ensure!(
            temp.metadata()?.len() == normalized_len,
            "normalized Active v8 WAL has an unexpected length"
        );
        temp.flush()
            .context("flush normalized Active v8 recovery image before verification")?;
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::wal_crash_point("wal_v8.recovery.after_temp_write_before_sync");
        let mut verification_file =
            File::open(&temp_path).context("open normalized Active v8 image for verification")?;
        let verification_discovery = discover_frontier_candidates(
            &mut verification_file,
            normalized_len,
            &header_digest,
            &initial_prefix_digest,
        )?;
        let verified = select_active_candidate_for_validation(
            &mut verification_file,
            normalized_len,
            &verification_discovery,
            &selection.hard_anchors,
            selection.header.predecessor,
            limits,
        )?;
        ensure!(
            verified.active_boundary == selection.active_boundary
                && verified.candidate.frontier == frontier,
            "normalized Active v8 image does not reproduce its selected logical boundary"
        );
        drop(verification_file);
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::fail_point!("wal_v8.recovery.before_temp_sync", |_| Err(
            anyhow::anyhow!("injected Active v8 recovery image sync failure")
        ));
        temp.sync_all()
            .context("sync normalized Active v8 recovery image")?;
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::wal_crash_point("wal_v8.recovery.after_temp_sync_before_rename");
        drop(temp);

        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::fail_point!("wal_v8.recovery.before_rename", |_| Err(
            anyhow::anyhow!("injected Active v8 recovery rename failure")
        ));
        fs::rename(&temp_path, path).context("atomically replace Active v8 WAL")?;
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::wal_crash_point(
            "wal_v8.recovery.after_rename_before_directory_sync",
        );
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::fail_point!("wal_v8.recovery.before_directory_sync", |_| Err(
            anyhow::anyhow!("injected Active v8 recovery directory sync failure")
        ));
        parent_dir
            .sync_all()
            .context("sync Active v8 WAL directory after replacement")?;
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::wal_crash_point("wal_v8.recovery.after_directory_sync");
        let installed_metadata =
            fs::symlink_metadata(path).context("inspect installed Active v8 WAL inode")?;
        ensure!(
            installed_metadata.file_type().is_file() && installed_metadata.len() == normalized_len,
            "installed Active v8 WAL has an unexpected file type or length"
        );

        let last_recorded_at = replay_tickets.last().map(|ticket| ticket.recorded_at);
        Ok(WalV8InstalledRecovery {
            next_ticket: selection.active_boundary.ticket_end,
            last_recorded_at,
            active_boundary: selection.active_boundary,
            header_digest,
            frontier,
            frontier_offset,
            frontier_digest,
            image_len: normalized_len,
            installed_dev: installed_metadata.dev(),
            installed_ino: installed_metadata.ino(),
            replay_tickets,
            selection,
        })
    })();

    match result {
        Err(operation_error) if temp_created => {
            match cleanup_failed_recovery_image(&temp_path, &parent_dir) {
                Ok(()) => Err(operation_error),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "{operation_error:#}; additionally, failed to durably clean the temporary Active v8 recovery image: {cleanup_error:#}"
                )),
            }
        }
        other => other,
    }
}

/// Replay batches from a normalized image after checking that the installed
/// inode still matches the recovery result. Only one decoded batch is retained
/// at a time while the caller applies it to the recovered memtable.
pub(crate) fn replay_installed_active_batches(
    path: impl AsRef<Path>,
    installed: &WalV8InstalledRecovery,
    limits: WalV5Limits,
    apply: impl FnMut(&crate::pitr::WalBatch) -> Result<()>,
) -> Result<()> {
    let path = path.as_ref();
    let path_metadata = fs::symlink_metadata(path).context("inspect installed Active v8 WAL")?;
    ensure!(
        path_metadata.file_type().is_file(),
        "installed Active v8 WAL path is not a regular file"
    );
    ensure!(
        path_metadata.dev() == installed.installed_dev
            && path_metadata.ino() == installed.installed_ino
            && path_metadata.len() == installed.image_len,
        "installed Active v8 WAL changed before memtable replay"
    );
    let mut file = File::open(path).context("open installed Active v8 WAL for replay")?;
    let file_metadata = file
        .metadata()
        .context("inspect opened Active v8 WAL for replay")?;
    ensure!(
        file_metadata.is_file()
            && file_metadata.dev() == installed.installed_dev
            && file_metadata.ino() == installed.installed_ino
            && file_metadata.len() == installed.image_len,
        "opened Active v8 WAL differs from the installed recovery inode"
    );
    let mut header_bytes = [0; super::WAL_V8_HEADER_LEN];
    file.seek(SeekFrom::Start(0))
        .context("seek installed Active v8 WAL header for replay")?;
    file.read_exact(&mut header_bytes)
        .context("read installed Active v8 WAL header for replay")?;
    let header = WalV8Header::decode(&header_bytes)?;
    ensure!(
        header.encode()? == header_bytes && header == installed.selection.header,
        "installed Active v8 WAL header changed before memtable replay"
    );
    let header_digest: [u8; 32] = Sha256::digest(header_bytes).into();
    ensure!(
        header_digest == installed.header_digest
            && header_digest == installed.selection.physical_header_digest,
        "installed Active v8 WAL header digest changed before memtable replay"
    );
    replay_selected_batches(
        &mut file,
        &installed.selection,
        &installed.replay_tickets,
        limits,
        apply,
    )
}

fn validate_selected_candidate(
    source: &mut File,
    source_len: u64,
    selection: &WalV8RecoverySelection,
    header_digest: [u8; 32],
    initial_prefix_digest: super::digest::WalV8Digest,
) -> Result<()> {
    let candidate_end = selection
        .candidate
        .frame_offset
        .checked_add(FRAME_LEN_U64)
        .context("selected Active v8 FRONTIER end overflows")?;
    ensure!(
        candidate_end <= source_len,
        "selected Active v8 FRONTIER exceeds the source image"
    );
    let mut bytes = [0; WAL_V8_FRAME_LEN];
    source
        .seek(SeekFrom::Start(selection.candidate.frame_offset))
        .context("seek selected Active v8 FRONTIER")?;
    source
        .read_exact(&mut bytes)
        .context("read selected Active v8 FRONTIER")?;
    ensure!(
        frame_record_digest(&bytes)? == selection.candidate.frame_digest,
        "selected Active v8 FRONTIER changed after selection"
    );
    let decoded = decode_frame_structural(
        &bytes,
        selection.candidate.frame_offset,
        &header_digest,
        &initial_prefix_digest,
    )?;
    ensure!(
        decoded.kind == super::codec::WalV8FrameKind::Frontier
            && WalV8Frontier::decode_body(&decoded.body)? == selection.candidate.frontier,
        "selected Active v8 FRONTIER contents changed after selection"
    );
    Ok(())
}

fn copy_retained_prefix(
    source: &mut File,
    destination: &mut File,
    mut remaining: u64,
) -> Result<()> {
    source
        .seek(SeekFrom::Start(HEADER_LEN_U64))
        .context("seek Active v8 retained prefix")?;
    let mut buffer = [0; COPY_BUFFER_LEN];
    while remaining > 0 {
        let chunk_len = usize::try_from(remaining.min(COPY_BUFFER_LEN as u64))?;
        source
            .read_exact(&mut buffer[..chunk_len])
            .context("read Active v8 retained prefix")?;
        destination
            .write_all(&buffer[..chunk_len])
            .context("write normalized Active v8 retained prefix")?;
        remaining -= u64::try_from(chunk_len)?;
    }
    Ok(())
}

fn recovery_temp_prefix(file_name: &std::ffi::OsStr) -> String {
    let digest = Sha256::digest(file_name.as_encoded_bytes());
    let mut output = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    format!(".toy-kv-v8-recovery-{output}.tmp-")
}

fn cleanup_stale_recovery_images(parent: &Path, parent_dir: &File, prefix: &str) -> Result<()> {
    for entry in fs::read_dir(parent).context("scan for stale v8 recovery images")? {
        let entry = entry.context("read v8 recovery directory entry")?;
        if !entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(prefix.as_bytes())
        {
            continue;
        }
        if !entry
            .file_type()
            .context("inspect stale v8 recovery image")?
            .is_file()
        {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("remove stale v8 recovery image"),
        }
    }
    parent_dir
        .sync_all()
        .context("sync directory after checking stale v8 recovery images")
}

fn cleanup_failed_recovery_image(temp_path: &Path, parent_dir: &File) -> Result<()> {
    match fs::remove_file(temp_path) {
        Ok(()) => parent_dir
            .sync_all()
            .context("sync directory after removing failed v8 recovery image"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => parent_dir
            .sync_all()
            .context("sync directory after confirming failed v8 recovery image is absent"),
        Err(error) => Err(error).context("remove failed v8 recovery image"),
    }
}
