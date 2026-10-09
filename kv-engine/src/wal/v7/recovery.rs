//! Bounded backward discovery of v7 WAL frontier candidates.
//!
//! Discovery is deliberately independent of DATA decoding. Its results are
//! structural candidates only; recovery must still verify each candidate's
//! covered prefix, frontier chain, authority, and durable anchors before it can
//! select or install a boundary.

use std::io::{Read, Seek, SeekFrom};

use anyhow::{Context, Result, ensure};

use super::codec::{
    DecodedWalV7Frame, WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WalV7FrameKind, WalV7Frontier,
    decode_frame_structural, frame_record_digest, is_frontier_frame_header,
};
use crate::wal::MAX_WAL_FILE_SIZE;

const DISCOVERY_BATCH_FRAMES: usize = 64;
const MAX_REJECTED_FRONTIER_DIAGNOSTICS: usize = 64;
const MAX_REJECTION_REASON_CHARS: usize = 192;

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

    use super::*;
    use crate::wal::v7::codec::{
        WalV7Frontier, decode_frame_structural, encode_frontier_successor,
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
}
