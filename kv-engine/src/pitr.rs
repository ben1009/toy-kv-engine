//! Crate-private contracts and codec primitives for PITR WAL v5.
//!
//! This module is deliberately dormant: the existing WAL writer and recovery
//! paths do not select v5 yet. The types here provide one canonical envelope
//! for the later sequencer, segment manager, and PITR replay implementation.
#![allow(dead_code)]

use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};

pub(crate) const WAL_V5_MAGIC: [u8; 4] = *b"WAL2";
/// The v5-family version new segments claim: the v5 batch layout, with a digest
/// that covers only each batch's own bytes. See [`WalDigestRule`].
pub(crate) const WAL_V5_VERSION: u16 = 6;
/// Segments written before the logical-batch digest existed. Read and verified
/// under the rule they were written with, forever.
pub(crate) const WAL_V5_VERSION_LEGACY: u16 = 5;
pub(crate) const WAL_V5_HEADER_LEN: usize = 4096;
pub(crate) const WAL_V5_BATCH_HEADER_LEN: usize = 40;
pub(crate) const WAL_V5_ALIGNMENT: usize = 4096;

/// Which bytes a segment's `wal_digest` covers. This is the only difference
/// between a v5 and a v6 segment; the batch layout is identical.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalDigestRule {
    /// Every byte of the aligned prefix, alignment padding included.
    WholeAlignedPrefix,
    /// The file header plus each batch's own bytes, skipping the padding
    /// `encode_v5_batch` adds for `O_DIRECT` alignment.
    LogicalBatches,
}

impl WalDigestRule {
    /// The part of one batch a digest covers: the batch's whole aligned extent, or
    /// just its own bytes. Both the walk over a file and the live accumulator call
    /// this, so the digest a segment accumulates while it is written and the digest
    /// a rebuild derives from the same bytes cannot disagree about the coverage.
    ///
    /// `extent` is the batch's `WAL_V5_BATCH_HEADER_LEN + data_len` .. aligned end
    /// slice, `logical_len` the header plus data length it carries.
    pub(crate) fn batch_slice(self, extent: &[u8], logical_len: usize) -> &[u8] {
        match self {
            Self::WholeAlignedPrefix => extent,
            Self::LogicalBatches => &extent[..logical_len],
        }
    }
}

/// Both v5-family versions are read forever; only [`WAL_V5_VERSION`] is written.
pub(crate) fn is_v5_family(version: u16) -> bool {
    matches!(version, WAL_V5_VERSION | WAL_V5_VERSION_LEGACY)
}

pub(crate) fn wal_digest_rule(version: u16) -> Result<WalDigestRule> {
    match version {
        WAL_V5_VERSION => Ok(WalDigestRule::LogicalBatches),
        WAL_V5_VERSION_LEGACY => Ok(WalDigestRule::WholeAlignedPrefix),
        other => bail!("unsupported v5-family WAL version {other}"),
    }
}

pub(crate) const LIVE_WAL_V5_LIMITS: WalV5Limits = WalV5Limits {
    max_input_entry_count: 1 << 20,
    max_batch_data_bytes: 128 << 20,
    max_entry_count: 1 << 20,
    max_key_bytes: u16::MAX as usize,
    max_value_bytes: u16::MAX as usize,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV5Limits {
    pub(crate) max_input_entry_count: usize,
    pub(crate) max_batch_data_bytes: usize,
    pub(crate) max_entry_count: usize,
    pub(crate) max_key_bytes: usize,
    pub(crate) max_value_bytes: usize,
}

impl WalV5Limits {
    fn validate(self) -> Result<Self> {
        ensure!(
            self.max_input_entry_count > 0,
            "v5 input entry count limit must be nonzero"
        );
        ensure!(
            self.max_batch_data_bytes > 0,
            "v5 batch byte limit must be nonzero"
        );
        ensure!(
            self.max_batch_data_bytes <= u32::MAX as usize,
            "v5 batch byte limit exceeds wire format"
        );
        ensure!(
            self.max_entry_count > 0,
            "v5 entry count limit must be nonzero"
        );
        ensure!(
            self.max_entry_count <= u32::MAX as usize,
            "v5 entry count limit exceeds wire format"
        );
        ensure!(
            self.max_input_entry_count >= self.max_entry_count,
            "v5 input entry count limit is below wire entry count limit"
        );
        ensure!(
            self.max_key_bytes <= u32::MAX as usize,
            "v5 key limit exceeds wire format"
        );
        ensure!(
            self.max_value_bytes <= u32::MAX as usize,
            "v5 value limit exceeds wire format"
        );
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct TimelineId(pub(crate) [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ArchiveEpochId(pub(crate) [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct SegmentId(pub(crate) u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct CommitTs(pub(crate) u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct CommitTicket(pub(crate) u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitReservation {
    pub(crate) commit_ts: CommitTs,
    pub(crate) ticket: CommitTicket,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentAnchor {
    pub(crate) segment_id: SegmentId,
    pub(crate) wal_digest: [u8; 32],
    pub(crate) seal_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChainAnchor {
    Genesis { archive_epoch_id: ArchiveEpochId },
    Segment(SegmentAnchor),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitTimeHighWater {
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) segment_id: SegmentId,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
    pub(crate) entry_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RecordedAt {
    pub(crate) secs: i64,
    pub(crate) nanos: u32,
}

impl RecordedAt {
    pub(crate) fn from_system_time(time: SystemTime) -> Result<Self> {
        match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => Ok(Self {
                secs: i64::try_from(duration.as_secs())
                    .context("recorded_at seconds exceed supported range")?,
                nanos: duration.subsec_nanos(),
            }),
            Err(error) => {
                let duration = error.duration();
                let seconds = duration.as_secs();
                if duration.subsec_nanos() == 0 {
                    let secs = if seconds == 1_u64 << 63 {
                        i64::MIN
                    } else {
                        i64::try_from(seconds)
                            .context("recorded_at seconds exceed supported range")?
                            .checked_neg()
                            .context("recorded_at seconds exceed supported range")?
                    };
                    Ok(Self { secs, nanos: 0 })
                } else {
                    let seconds = i64::try_from(seconds)
                        .context("recorded_at seconds exceed supported range")?;
                    Ok(Self {
                        secs: -seconds - 1,
                        nanos: 1_000_000_000 - duration.subsec_nanos(),
                    })
                }
            }
        }
    }

    pub(crate) fn as_system_time(self) -> Result<SystemTime> {
        ensure!(self.nanos < 1_000_000_000, "recorded_at nanos out of range");
        if self.secs >= 0 {
            let duration = Duration::from_secs(self.secs as u64)
                .checked_add(Duration::from_nanos(u64::from(self.nanos)))
                .context("recorded_at duration overflow")?;
            UNIX_EPOCH
                .checked_add(duration)
                .context("recorded_at after supported SystemTime range")
        } else {
            let positive = Duration::from_secs(self.secs.unsigned_abs());
            let nanos = Duration::from_nanos(u64::from(self.nanos));
            positive
                .checked_sub(nanos)
                .and_then(|duration| UNIX_EPOCH.checked_sub(duration))
                .context("recorded_at before supported SystemTime range")
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    PointDelete { key: Vec<u8> },
    RangeDelete { start: Vec<u8>, end: Vec<u8> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalBatch {
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: RecordedAt,
    pub(crate) entries: Vec<WalEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV5Header {
    /// The v5-family version on the wire: [`WAL_V5_VERSION`] for segments written
    /// now, [`WAL_V5_VERSION_LEGACY`] for one written before the logical-batch
    /// digest existed. Carried on the header rather than read from the code so that
    /// every path holding a header - including a resume of an older segment - has
    /// the segment's own version, and so the seal can echo it.
    pub(crate) wal_format_version: u16,
    pub(crate) timeline_id: TimelineId,
    pub(crate) archive_epoch_id: ArchiveEpochId,
    pub(crate) segment_id: SegmentId,
    pub(crate) predecessor: ChainAnchor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV5BatchHeader {
    pub(crate) commit_ts: CommitTs,
    pub(crate) recorded_at: RecordedAt,
    pub(crate) entry_count: u32,
    pub(crate) data_len: u32,
    pub(crate) data_crc32: u32,
    pub(crate) header_crc32: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedBatch {
    pub(crate) batch: WalBatch,
    /// End of the batch's own bytes, before the alignment padding that follows.
    pub(crate) data_end: usize,
    pub(crate) logical_end: usize,
}

#[derive(Debug)]
pub(crate) enum V5BatchDecodeError {
    Truncated(&'static str),
    CrcMismatch(&'static str),
}

impl std::fmt::Display for V5BatchDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated(reason) => write!(formatter, "truncated v5 batch {reason}"),
            Self::CrcMismatch(reason) => write!(formatter, "v5 batch {reason} CRC mismatch"),
        }
    }
}

impl std::error::Error for V5BatchDecodeError {}

pub(crate) fn encode_v5_file_header(header: WalV5Header) -> Result<[u8; WAL_V5_HEADER_LEN]> {
    let mut output = [0; WAL_V5_HEADER_LEN];
    output[0..4].copy_from_slice(&WAL_V5_MAGIC);
    ensure!(
        is_v5_family(header.wal_format_version),
        "unsupported v5 WAL version {}",
        header.wal_format_version
    );
    output[4..6].copy_from_slice(&header.wal_format_version.to_be_bytes());
    output[8..10].copy_from_slice(&(WAL_V5_HEADER_LEN as u16).to_be_bytes());
    output[12..28].copy_from_slice(&header.timeline_id.0);
    output[28..44].copy_from_slice(&header.archive_epoch_id.0);
    output[44..52].copy_from_slice(&header.segment_id.0.to_be_bytes());
    match header.predecessor {
        ChainAnchor::Genesis { archive_epoch_id } => {
            ensure!(
                archive_epoch_id == header.archive_epoch_id,
                "genesis anchor epoch does not match WAL header epoch"
            );
        }
        ChainAnchor::Segment(anchor) => {
            output[52] = 1;
            output[56..64].copy_from_slice(&anchor.segment_id.0.to_be_bytes());
            output[64..96].copy_from_slice(&anchor.wal_digest);
            output[96..128].copy_from_slice(&anchor.seal_digest);
        }
    }
    let header_crc = crc32fast::hash(&output[..128]);
    output[128..132].copy_from_slice(&header_crc.to_be_bytes());
    Ok(output)
}

pub(crate) fn decode_v5_file_header(input: &[u8]) -> Result<WalV5Header> {
    ensure!(input.len() >= WAL_V5_HEADER_LEN, "truncated v5 WAL header");
    ensure!(input[0..4] == WAL_V5_MAGIC, "invalid v5 WAL magic");
    ensure!(
        is_v5_family(u16::from_be_bytes([input[4], input[5]])),
        "invalid v5 WAL version"
    );
    ensure!(
        u16::from_be_bytes([input[6], input[7]]) == 0,
        "unknown v5 WAL flags"
    );
    ensure!(
        u16::from_be_bytes([input[8], input[9]]) as usize == WAL_V5_HEADER_LEN,
        "invalid v5 header length"
    );
    ensure!(
        u16::from_be_bytes([input[10], input[11]]) == 0,
        "nonzero v5 reserved field"
    );
    ensure!(
        input[132..WAL_V5_HEADER_LEN] == [0; WAL_V5_HEADER_LEN - 132],
        "nonzero v5 reserved bytes"
    );
    ensure!(
        crc32fast::hash(&input[..128]) == u32::from_be_bytes(input[128..132].try_into().unwrap()),
        "v5 header CRC mismatch"
    );
    let predecessor = match input[52] {
        0 => {
            ensure!(
                input[56..128] == [0; 72],
                "nonzero genesis predecessor fields"
            );
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId(input[28..44].try_into().unwrap()),
            }
        }
        1 => ChainAnchor::Segment(SegmentAnchor {
            segment_id: SegmentId(u64::from_be_bytes(input[56..64].try_into().unwrap())),
            wal_digest: input[64..96].try_into().unwrap(),
            seal_digest: input[96..128].try_into().unwrap(),
        }),
        _ => bail!("invalid v5 predecessor kind"),
    };
    ensure!(
        input[53..56] == [0; 3],
        "nonzero v5 predecessor reserved bytes"
    );
    Ok(WalV5Header {
        wal_format_version: u16::from_be_bytes([input[4], input[5]]),
        timeline_id: TimelineId(input[12..28].try_into().unwrap()),
        archive_epoch_id: ArchiveEpochId(input[28..44].try_into().unwrap()),
        segment_id: SegmentId(u64::from_be_bytes(input[44..52].try_into().unwrap())),
        predecessor,
    })
}

pub(crate) fn encode_v5_batch(batch: &WalBatch, limits: WalV5Limits) -> Result<Vec<u8>> {
    let canonical = canonical_batch(batch, limits)?;
    let len = v5_batch_encoded_len(&canonical, limits)?;
    let mut out = vec![0_u8; len];
    let written = encode_v5_batch_into(&canonical, limits, &mut out)?;
    debug_assert_eq!(written, len);
    Ok(out)
}

/// Canonicalise only when there is something to drop.
///
/// Canonicalisation keeps the last point write per key, which takes a map and a
/// clone of every surviving entry. A batch with fewer than two entries cannot
/// contain a repeated point key, and a single `put` is exactly that batch, so
/// the common case skips both.
pub(crate) fn canonical_batch(batch: &WalBatch, limits: WalV5Limits) -> Result<Cow<'_, WalBatch>> {
    // The input count is checked before the duplicate scan below, so a batch of
    // repeated keys cannot make that scan unbounded work. It is a different
    // bound from `max_entry_count`, which applies to what survives.
    let limits = limits.validate()?;
    ensure!(
        batch.entries.len() <= limits.max_input_entry_count,
        "v5 input entry count exceeds configured limit"
    );
    if batch.entries.len() <= 1 {
        return Ok(Cow::Borrowed(batch));
    }
    Ok(Cow::Owned(batch.canonicalized()?))
}

/// The length one encoded batch occupies: its fixed header, its entries, and the
/// zero padding up to the `O_DIRECT` alignment.
pub(crate) fn v5_batch_encoded_len(batch: &WalBatch, limits: WalV5Limits) -> Result<usize> {
    let limits = limits.validate()?;
    validate_encoding_inputs(batch, limits)?;
    align_up(WAL_V5_BATCH_HEADER_LEN + batch_data_len(batch, limits)?)
}

/// Encode one batch straight into the buffer the ring will read.
///
/// `dst` must have room for [`v5_batch_encoded_len`]; the entries are written in
/// place and hashed as they are written, so the data checksum needs no second
/// pass over them and no intermediate buffer is allocated.
pub(crate) fn encode_v5_batch_into(
    batch: &WalBatch,
    limits: WalV5Limits,
    dst: &mut [u8],
) -> Result<usize> {
    let limits = limits.validate()?;
    validate_encoding_inputs(batch, limits)?;
    let data_len = batch_data_len(batch, limits)?;
    let logical_len = WAL_V5_BATCH_HEADER_LEN + data_len;
    let aligned_len = align_up(logical_len)?;
    ensure!(
        dst.len() >= aligned_len,
        "v5 batch output buffer is too small"
    );

    // Header. Fields are written by position, so the only ordering that matters
    // is that the bytes the header checksum covers are in place before it is
    // taken; the data checksum is written after the entries it covers. The
    // reserved tail is zeroed first because the buffer may be a pooled one.
    dst[..WAL_V5_BATCH_HEADER_LEN].fill(0);
    dst[0..8].copy_from_slice(&batch.commit_ts.to_be_bytes());
    dst[8..16].copy_from_slice(&batch.recorded_at.secs.to_be_bytes());
    dst[16..20].copy_from_slice(&batch.recorded_at.nanos.to_be_bytes());
    dst[20..24].copy_from_slice(&(batch.entries.len() as u32).to_be_bytes());
    dst[24..28].copy_from_slice(
        &u32::try_from(data_len)
            .context("v5 batch data too large")?
            .to_be_bytes(),
    );
    let header_crc = crc32fast::hash(&dst[..28]);
    dst[32..36].copy_from_slice(&header_crc.to_be_bytes());

    let mut hasher = crc32fast::Hasher::new();
    let mut pos = WAL_V5_BATCH_HEADER_LEN;
    for entry in &batch.entries {
        pos = write_entry_into(dst, pos, entry, &mut hasher)?;
    }
    debug_assert_eq!(pos, logical_len);
    dst[28..32].copy_from_slice(&hasher.finalize().to_be_bytes());

    dst[pos..aligned_len].fill(0);
    Ok(aligned_len)
}

/// Everything the encoder refuses before it writes a byte.
fn validate_encoding_inputs(batch: &WalBatch, limits: WalV5Limits) -> Result<()> {
    ensure!(batch.commit_ts != 0, "v5 commit timestamp must be nonzero");
    ensure!(!batch.entries.is_empty(), "v5 batch must contain an entry");
    ensure!(
        batch.recorded_at.nanos < 1_000_000_000,
        "recorded_at nanos out of range"
    );
    ensure!(
        batch.entries.len() <= limits.max_entry_count,
        "v5 entry count exceeds configured limit"
    );

    Ok(())
}

/// Write one entry at `pos`, feeding the same bytes to the data checksum.
fn write_entry_into(
    dst: &mut [u8],
    pos: usize,
    entry: &WalEntry,
    hasher: &mut crc32fast::Hasher,
) -> Result<usize> {
    let (kind, fields): (u8, &[&[u8]]) = match entry {
        WalEntry::Put { key, value } => (1, &[key.as_slice(), value.as_slice()]),
        WalEntry::PointDelete { key } => (2, &[key.as_slice()]),
        WalEntry::RangeDelete { start, end } => (3, &[start.as_slice(), end.as_slice()]),
    };
    let payload_len: usize = fields.iter().map(|field| 4 + field.len()).sum();
    let mut header = [0_u8; 6];
    header[0] = kind;
    header[2..6].copy_from_slice(
        &u32::try_from(payload_len)
            .context("v5 payload too large")?
            .to_be_bytes(),
    );
    let mut at = pos;
    dst[at..at + header.len()].copy_from_slice(&header);
    hasher.update(&header);
    at += header.len();
    for field in fields {
        let prefix = u32::try_from(field.len())
            .context("v5 field too large")?
            .to_be_bytes();
        dst[at..at + prefix.len()].copy_from_slice(&prefix);
        hasher.update(&prefix);
        at += prefix.len();
        dst[at..at + field.len()].copy_from_slice(field);
        hasher.update(field);
        at += field.len();
    }

    Ok(at)
}

/// The encoder as it was before it learned to write into the caller's buffer:
/// it builds the batch in a `Vec` and does not canonicalise, so it is the only
/// way to encode an uncanonicalised batch.
///
/// Production has exactly one encoder - [`encode_v5_batch_into`], called directly
/// from the WAL's `put_v5_batch` - and neither this function nor the
/// [`encode_v5_batch`] wrapper above it is on the write path any more: every
/// caller of the wrapper is a test. Both are kept as fixtures for `pitr::tests`,
/// this one because a decoder test needs a builder that preserves duplicate keys,
/// and the file-level `allow(dead_code)` above is what keeps them compiling. The
/// pinned digests in that module were produced by this implementation, which is
/// why they can be checked against it.
fn encode_v5_batch_inner(batch: &WalBatch, limits: WalV5Limits) -> Result<Vec<u8>> {
    validate_batch_limits(batch, limits)?;
    ensure!(batch.commit_ts != 0, "v5 commit timestamp must be nonzero");
    ensure!(!batch.entries.is_empty(), "v5 batch must contain an entry");
    ensure!(
        batch.recorded_at.nanos < 1_000_000_000,
        "recorded_at nanos out of range"
    );
    let mut data = Vec::new();
    for entry in &batch.entries {
        let encoded_len = encoded_entry_len(entry, limits)?;
        let projected_len = data
            .len()
            .checked_add(encoded_len)
            .context("v5 batch size overflow")?;
        ensure!(
            projected_len <= limits.max_batch_data_bytes,
            "v5 batch data exceeds configured limit"
        );
        let (kind, payload) = match entry {
            WalEntry::Put { key, value } => {
                let mut payload = Vec::with_capacity(encoded_len - 6);
                put_len_prefixed(&mut payload, key)?;
                put_len_prefixed(&mut payload, value)?;
                (1, payload)
            }
            WalEntry::PointDelete { key } => {
                let mut payload = Vec::with_capacity(encoded_len - 6);
                put_len_prefixed(&mut payload, key)?;
                (2, payload)
            }
            WalEntry::RangeDelete { start, end } => {
                ensure!(start < end, "invalid range tombstone ordering");
                let mut payload = Vec::with_capacity(encoded_len - 6);
                put_len_prefixed(&mut payload, start)?;
                put_len_prefixed(&mut payload, end)?;
                (3, payload)
            }
        };
        data.push(kind);
        data.push(0);
        data.extend_from_slice(
            &(u32::try_from(payload.len()).context("v5 payload too large")?).to_be_bytes(),
        );
        data.extend_from_slice(&payload);
    }
    let data_len = u32::try_from(data.len()).context("v5 batch data too large")?;
    let mut output = vec![0; WAL_V5_BATCH_HEADER_LEN];
    output[0..8].copy_from_slice(&batch.commit_ts.to_be_bytes());
    output[8..16].copy_from_slice(&batch.recorded_at.secs.to_be_bytes());
    output[16..20].copy_from_slice(&batch.recorded_at.nanos.to_be_bytes());
    output[20..24].copy_from_slice(&(batch.entries.len() as u32).to_be_bytes());
    output[24..28].copy_from_slice(&data_len.to_be_bytes());
    output[28..32].copy_from_slice(&crc32fast::hash(&data).to_be_bytes());
    let header_crc = crc32fast::hash(&output[..28]);
    output[32..36].copy_from_slice(&header_crc.to_be_bytes());
    output.extend_from_slice(&data);
    let aligned_len = align_up(output.len())?;
    output.resize(aligned_len, 0);
    Ok(output)
}

/// The logical length of an encoded batch: its fixed header plus the data length
/// the header carries. Everything past that, up to the alignment boundary, is
/// padding the digest rules may or may not cover.
pub(crate) fn encoded_v5_batch_logical_len(encoded: &[u8]) -> Result<usize> {
    ensure!(
        encoded.len() >= WAL_V5_BATCH_HEADER_LEN,
        "truncated v5 batch header"
    );
    let data_len = u32::from_be_bytes(encoded[24..28].try_into().unwrap()) as usize;
    let logical_len = WAL_V5_BATCH_HEADER_LEN
        .checked_add(data_len)
        .context("v5 batch logical length overflow")?;
    ensure!(
        logical_len <= encoded.len(),
        "v5 batch data length exceeds its buffer"
    );
    Ok(logical_len)
}

impl WalBatch {
    pub(crate) fn canonicalized(&self) -> Result<Self> {
        ensure!(
            self.entries.len() <= u32::MAX as usize,
            "v5 entry count exceeds wire format"
        );
        let mut last_point = HashMap::new();
        for (index, entry) in self.entries.iter().enumerate() {
            if let WalEntry::Put { key, .. } | WalEntry::PointDelete { key } = entry {
                last_point.insert(key.as_slice(), index);
            }
        }
        let entries = self
            .entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| match entry {
                WalEntry::Put { key, .. } | WalEntry::PointDelete { key }
                    if last_point.get(key.as_slice()) != Some(&index) =>
                {
                    None
                }
                _ => Some(entry.clone()),
            })
            .collect();
        Ok(Self {
            commit_ts: self.commit_ts,
            recorded_at: self.recorded_at,
            entries,
        })
    }
}

pub(crate) fn decode_v5_batch(
    input: &[u8],
    offset: usize,
    limits: WalV5Limits,
) -> Result<DecodedBatch> {
    let limits = limits.validate()?;
    ensure!(
        offset.is_multiple_of(WAL_V5_ALIGNMENT),
        "v5 batch offset is not aligned"
    );
    let header_end = offset
        .checked_add(WAL_V5_BATCH_HEADER_LEN)
        .context("v5 batch offset overflow")?;
    if input.len() < header_end {
        return Err(anyhow::Error::new(V5BatchDecodeError::Truncated("header")));
    }
    let header = &input[offset..header_end];
    let commit_ts = u64::from_be_bytes(header[0..8].try_into().unwrap());
    let recorded_at = RecordedAt {
        secs: i64::from_be_bytes(header[8..16].try_into().unwrap()),
        nanos: u32::from_be_bytes(header[16..20].try_into().unwrap()),
    };
    let entry_count = u32::from_be_bytes(header[20..24].try_into().unwrap()) as usize;
    let data_len = u32::from_be_bytes(header[24..28].try_into().unwrap()) as usize;
    ensure!(
        commit_ts != 0 && entry_count != 0 && data_len != 0,
        "invalid empty v5 batch"
    );
    ensure!(
        entry_count <= limits.max_entry_count,
        "v5 entry count exceeds configured limit"
    );
    ensure!(
        data_len <= limits.max_batch_data_bytes,
        "v5 batch data exceeds configured limit"
    );
    ensure!(
        recorded_at.nanos < 1_000_000_000,
        "recorded_at nanos out of range"
    );
    if u32::from_be_bytes(header[32..36].try_into().unwrap()) != crc32fast::hash(&header[..28]) {
        return Err(anyhow::Error::new(V5BatchDecodeError::CrcMismatch(
            "header",
        )));
    }
    let data_start = header_end;
    let data_end = data_start
        .checked_add(data_len)
        .context("v5 batch data length overflow")?;
    let data = input
        .get(data_start..data_end)
        .ok_or_else(|| anyhow::Error::new(V5BatchDecodeError::Truncated("data")))?;
    if u32::from_be_bytes(header[28..32].try_into().unwrap()) != crc32fast::hash(data) {
        return Err(anyhow::Error::new(V5BatchDecodeError::CrcMismatch("data")));
    }
    ensure!(
        u32::from_be_bytes(header[36..40].try_into().unwrap()) == 0,
        "nonzero v5 batch reserved field"
    );
    let mut cursor: usize = 0;
    let mut entries = Vec::with_capacity(entry_count.min(data_len / 6));
    for _ in 0..entry_count {
        let entry_header_end = cursor
            .checked_add(6)
            .context("v5 entry header length overflow")?;
        if entry_header_end > data.len() {
            return Err(anyhow::Error::new(V5BatchDecodeError::Truncated(
                "entry header",
            )));
        }
        let kind = data[cursor];
        ensure!(data[cursor + 1] == 0, "unknown v5 entry flags");
        let payload_len =
            u32::from_be_bytes(data[cursor + 2..cursor + 6].try_into().unwrap()) as usize;
        cursor += 6;
        let payload_end = cursor
            .checked_add(payload_len)
            .context("v5 entry payload length overflow")?;
        let payload = data
            .get(cursor..payload_end)
            .ok_or_else(|| anyhow::Error::new(V5BatchDecodeError::Truncated("entry payload")))?;
        entries.push(decode_entry(kind, payload, limits)?);
        cursor += payload_len;
    }
    ensure!(cursor == data.len(), "v5 batch has trailing data");
    let logical_end = align_up(data_end)?;
    if input.len() < logical_end {
        return Err(anyhow::Error::new(V5BatchDecodeError::Truncated(
            "alignment gap",
        )));
    }
    ensure!(
        input[data_end..logical_end].iter().all(|byte| *byte == 0),
        "nonzero v5 alignment gap"
    );
    let decoded_batch = WalBatch {
        commit_ts,
        recorded_at,
        entries,
    };
    ensure!(
        decoded_batch.canonicalized()?.entries == decoded_batch.entries,
        "v5 batch is not canonical"
    );
    Ok(DecodedBatch {
        batch: decoded_batch,
        data_end,
        logical_end,
    })
}

pub(crate) fn commit_time_entry_digest(high_water: CommitTimeHighWater) -> [u8; 32] {
    let mut preimage = Vec::with_capacity(16 + 8 + 8 + 8 + 4 + 20);
    preimage.extend_from_slice(b"PITR-COMMIT-TIME-V1");
    preimage.extend_from_slice(&high_water.archive_epoch_id.0);
    preimage.extend_from_slice(&high_water.segment_id.0.to_be_bytes());
    preimage.extend_from_slice(&high_water.commit_ts.to_be_bytes());
    preimage.extend_from_slice(&high_water.recorded_at.secs.to_be_bytes());
    preimage.extend_from_slice(&high_water.recorded_at.nanos.to_be_bytes());
    Sha256::digest(preimage).into()
}

fn decode_entry(kind: u8, payload: &[u8], limits: WalV5Limits) -> Result<WalEntry> {
    let mut cursor = 0;
    let first = read_len_prefixed(payload, &mut cursor, limits.max_key_bytes, "key")?;
    let entry = match kind {
        1 => WalEntry::Put {
            key: first,
            value: read_len_prefixed(payload, &mut cursor, limits.max_value_bytes, "value")?,
        },
        2 => WalEntry::PointDelete { key: first },
        3 => {
            let end = read_len_prefixed(payload, &mut cursor, limits.max_key_bytes, "range end")?;
            ensure!(first < end, "invalid range tombstone ordering");
            WalEntry::RangeDelete { start: first, end }
        }
        _ => bail!("unknown v5 entry kind"),
    };
    ensure!(
        cursor == payload.len(),
        "v5 entry payload has trailing bytes"
    );
    Ok(entry)
}

fn put_len_prefixed(output: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    output.extend_from_slice(
        &u32::try_from(bytes.len())
            .context("v5 field too large")?
            .to_be_bytes(),
    );
    output.extend_from_slice(bytes);
    Ok(())
}

fn read_len_prefixed(
    input: &[u8],
    cursor: &mut usize,
    max_len: usize,
    field: &str,
) -> Result<Vec<u8>> {
    let length_end = cursor.checked_add(4).context("v5 length offset overflow")?;
    let length = u32::from_be_bytes(
        input
            .get(*cursor..length_end)
            .context("truncated v5 length")?
            .try_into()
            .unwrap(),
    ) as usize;
    ensure!(length <= max_len, "v5 {field} exceeds configured limit");
    *cursor = length_end;
    let value_end = cursor
        .checked_add(length)
        .context("v5 field length overflow")?;
    let bytes = input
        .get(*cursor..value_end)
        .context("truncated v5 field")?
        .to_vec();
    *cursor = value_end;
    Ok(bytes)
}

fn validate_batch_limits(batch: &WalBatch, limits: WalV5Limits) -> Result<()> {
    ensure!(
        batch.entries.len() <= limits.max_entry_count,
        "v5 entry count exceeds configured limit"
    );
    batch_data_len(batch, limits)?;
    Ok(())
}

/// The size of a batch's entry section, which is what its header records as the
/// data length.
fn batch_data_len(batch: &WalBatch, limits: WalV5Limits) -> Result<usize> {
    let mut data_len = 0_usize;
    for entry in &batch.entries {
        data_len = data_len
            .checked_add(encoded_entry_len(entry, limits)?)
            .context("v5 batch size overflow")?;
        ensure!(
            data_len <= limits.max_batch_data_bytes,
            "v5 batch data exceeds configured limit"
        );
    }
    Ok(data_len)
}

fn encoded_entry_len(entry: &WalEntry, limits: WalV5Limits) -> Result<usize> {
    let payload_len = match entry {
        WalEntry::Put { key, value } => {
            validate_field_len(key, limits.max_key_bytes, "key")?;
            validate_field_len(value, limits.max_value_bytes, "value")?;
            8_usize
                .checked_add(key.len())
                .and_then(|length| length.checked_add(value.len()))
        }
        WalEntry::PointDelete { key } => {
            validate_field_len(key, limits.max_key_bytes, "key")?;
            4_usize.checked_add(key.len())
        }
        WalEntry::RangeDelete { start, end } => {
            ensure!(start < end, "invalid range tombstone ordering");
            validate_field_len(start, limits.max_key_bytes, "range start")?;
            validate_field_len(end, limits.max_key_bytes, "range end")?;
            8_usize
                .checked_add(start.len())
                .and_then(|length| length.checked_add(end.len()))
        }
    }
    .context("v5 entry payload size overflow")?;
    payload_len.checked_add(6).context("v5 entry size overflow")
}

fn validate_field_len(bytes: &[u8], max_len: usize, field: &str) -> Result<()> {
    ensure!(
        bytes.len() <= max_len,
        "v5 {field} exceeds configured limit"
    );
    Ok(())
}

fn align_up(value: usize) -> Result<usize> {
    let rounded = value
        .checked_add(WAL_V5_ALIGNMENT - 1)
        .context("v5 alignment length overflow")?;
    Ok(rounded / WAL_V5_ALIGNMENT * WAL_V5_ALIGNMENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> WalV5Header {
        WalV5Header {
            wal_format_version: crate::pitr::WAL_V5_VERSION,
            timeline_id: TimelineId([1; 16]),
            archive_epoch_id: ArchiveEpochId([2; 16]),
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Segment(SegmentAnchor {
                segment_id: SegmentId(6),
                wal_digest: [3; 32],
                seal_digest: [4; 32],
            }),
        }
    }

    fn batch() -> WalBatch {
        WalBatch {
            commit_ts: 11,
            recorded_at: RecordedAt {
                secs: -2,
                nanos: 500_000_000,
            },
            entries: vec![
                WalEntry::Put {
                    key: b"a".to_vec(),
                    value: b"one".to_vec(),
                },
                WalEntry::PointDelete { key: b"b".to_vec() },
                WalEntry::RangeDelete {
                    start: b"c".to_vec(),
                    end: b"d".to_vec(),
                },
            ],
        }
    }

    fn limits() -> WalV5Limits {
        WalV5Limits {
            max_input_entry_count: 32,
            max_batch_data_bytes: 1024,
            max_entry_count: 16,
            max_key_bytes: 32,
            max_value_bytes: 128,
        }
    }

    fn refresh_file_header_crc(bytes: &mut [u8]) {
        let crc = crc32fast::hash(&bytes[..128]);
        bytes[128..132].copy_from_slice(&crc.to_be_bytes());
    }

    fn refresh_batch_crcs(bytes: &mut [u8]) {
        let data_len = u32::from_be_bytes(bytes[24..28].try_into().unwrap()) as usize;
        let data_crc = crc32fast::hash(&bytes[40..40 + data_len]);
        bytes[28..32].copy_from_slice(&data_crc.to_be_bytes());
        let header_crc = crc32fast::hash(&bytes[..28]);
        bytes[32..36].copy_from_slice(&header_crc.to_be_bytes());
    }

    #[test]
    fn recorded_at_uses_floor_representation_before_epoch() {
        let time = UNIX_EPOCH - Duration::from_millis(1500);
        assert_eq!(
            RecordedAt::from_system_time(time).unwrap(),
            RecordedAt {
                secs: -2,
                nanos: 500_000_000
            }
        );
        assert_eq!(
            RecordedAt::from_system_time(time)
                .unwrap()
                .as_system_time()
                .unwrap(),
            time
        );
    }

    #[test]
    fn recorded_at_supports_i64_wire_boundaries() {
        for recorded_at in [
            RecordedAt {
                secs: i64::MIN,
                nanos: 0,
            },
            RecordedAt {
                secs: i64::MIN,
                nanos: 500_000_000,
            },
            RecordedAt {
                secs: i64::MAX,
                nanos: 999_999_999,
            },
        ] {
            let time = recorded_at.as_system_time().unwrap();
            assert_eq!(RecordedAt::from_system_time(time).unwrap(), recorded_at);
        }
    }

    #[test]
    fn genesis_anchor_epoch_must_match_file_header() {
        let mut value = header();
        value.predecessor = ChainAnchor::Genesis {
            archive_epoch_id: ArchiveEpochId([9; 16]),
        };
        assert!(encode_v5_file_header(value).is_err());
    }

    #[test]
    fn v5_genesis_header_round_trips() {
        let mut value = header();
        value.predecessor = ChainAnchor::Genesis {
            archive_epoch_id: value.archive_epoch_id,
        };
        let encoded = encode_v5_file_header(value).unwrap();
        assert_eq!(decode_v5_file_header(&encoded).unwrap(), value);
        assert!(encoded[56..128].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn v5_file_header_rejects_invalid_fixed_fields() {
        let valid = encode_v5_file_header(header()).unwrap();
        for index in [0, 4, 6, 8, 10, 52, 53, 128, 132] {
            let mut corrupted = valid;
            corrupted[index] ^= 1;
            assert!(decode_v5_file_header(&corrupted).is_err(), "index {index}");
        }

        let mut genesis = header();
        genesis.predecessor = ChainAnchor::Genesis {
            archive_epoch_id: genesis.archive_epoch_id,
        };
        let mut nonzero_predecessor = encode_v5_file_header(genesis).unwrap();
        nonzero_predecessor[56] = 1;
        refresh_file_header_crc(&mut nonzero_predecessor);
        assert!(decode_v5_file_header(&nonzero_predecessor).is_err());
    }

    /// The frozen v5 encoding, pinned byte for byte.
    ///
    /// Segments written before the logical-batch digest existed keep verifying under
    /// the rule and the version they were written with, forever, so this test exists
    /// to fail the day the legacy encoding moves - not merely the day a digest
    /// function does.
    #[test]
    fn v5_file_header_round_trips_and_has_zero_reserved_bytes() {
        let mut legacy = header();
        legacy.wal_format_version = WAL_V5_VERSION_LEGACY;
        let encoded = encode_v5_file_header(legacy).unwrap();
        assert_eq!(&encoded[..4], b"WAL2");
        assert_eq!(u16::from_be_bytes([encoded[4], encoded[5]]), 5);
        assert_eq!(&encoded[128..132], &[51, 214, 118, 33]);
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(encoded)),
            [
                4, 49, 157, 235, 192, 112, 41, 176, 130, 248, 199, 108, 122, 156, 35, 229, 90, 48,
                217, 8, 58, 127, 185, 253, 74, 232, 236, 252, 194, 8, 175, 222,
            ]
        );
        assert!(encoded[132..].iter().all(|byte| *byte == 0));
        assert_eq!(decode_v5_file_header(&encoded).unwrap(), legacy);
    }

    /// The same header at the version a segment written now claims: identical but
    /// for the version field, whose value is inside the header CRC.
    #[test]
    fn v6_file_header_differs_from_v5_only_in_the_version_field() {
        let mut current = header();
        current.wal_format_version = WAL_V5_VERSION;
        let encoded = encode_v5_file_header(current).unwrap();
        assert_eq!(u16::from_be_bytes([encoded[4], encoded[5]]), WAL_V5_VERSION);
        assert_eq!(decode_v5_file_header(&encoded).unwrap(), current);

        let mut legacy = header();
        legacy.wal_format_version = WAL_V5_VERSION_LEGACY;
        let legacy = encode_v5_file_header(legacy).unwrap();
        // The version field is inside the header CRC's preimage, so the CRC moves
        // with it and everything else stays put.
        assert_ne!(&encoded[..], &legacy[..]);
        assert_eq!(encoded[6..128], legacy[6..128]);
        assert_ne!(encoded[128..132], legacy[128..132]);
    }

    #[test]
    fn v5_family_versions_map_to_their_digest_rules() {
        assert_eq!(
            wal_digest_rule(WAL_V5_VERSION_LEGACY).unwrap(),
            WalDigestRule::WholeAlignedPrefix
        );
        assert_eq!(
            wal_digest_rule(WAL_V5_VERSION).unwrap(),
            WalDigestRule::LogicalBatches
        );
        assert!(is_v5_family(WAL_V5_VERSION_LEGACY));
        assert!(is_v5_family(WAL_V5_VERSION));
        assert!(!is_v5_family(4));
        assert!(wal_digest_rule(4).is_err());
    }

    #[test]
    fn v5_file_header_decoder_ignores_following_batch_bytes() {
        let mut wal = encode_v5_file_header(header()).unwrap().to_vec();
        wal.extend_from_slice(&encode_v5_batch(&batch(), limits()).unwrap());
        assert_eq!(decode_v5_file_header(&wal).unwrap(), header());
    }

    #[test]
    fn v5_batch_round_trips_and_is_aligned() {
        let encoded = encode_v5_batch(&batch(), limits()).unwrap();
        assert_eq!(encoded.len() % WAL_V5_ALIGNMENT, 0);
        assert_eq!(
            &encoded[..40],
            &[
                0, 0, 0, 0, 0, 0, 0, 11, 255, 255, 255, 255, 255, 255, 255, 254, 29, 205, 101, 0,
                0, 0, 0, 3, 0, 0, 0, 45, 36, 227, 69, 27, 45, 95, 145, 213, 0, 0, 0, 0,
            ]
        );
        assert_eq!(
            &encoded[40..85],
            &[
                1, 0, 0, 0, 0, 12, 0, 0, 0, 1, 97, 0, 0, 0, 3, 111, 110, 101, 2, 0, 0, 0, 0, 5, 0,
                0, 0, 1, 98, 3, 0, 0, 0, 0, 10, 0, 0, 0, 1, 99, 0, 0, 0, 1, 100,
            ]
        );
        assert!(encoded[85..].iter().all(|byte| *byte == 0));
        let decoded = decode_v5_batch(&encoded, 0, limits()).unwrap();
        assert_eq!(decoded.logical_end, encoded.len());
        assert_eq!(decoded.batch, batch());
    }

    #[test]
    fn v5_encoding_collapses_duplicate_point_operations_to_the_last() {
        let mut duplicate = batch();
        duplicate.entries.insert(
            1,
            WalEntry::Put {
                key: b"a".to_vec(),
                value: b"latest".to_vec(),
            },
        );
        let encoded = encode_v5_batch(&duplicate, limits()).unwrap();
        let decoded = decode_v5_batch(&encoded, 0, limits()).unwrap();
        assert_eq!(decoded.batch.entries[0], duplicate.entries[1]);
        assert_eq!(decoded.batch.entries.len(), 3);
    }

    #[test]
    fn v5_canonicalization_preserves_retained_mixed_entry_order() {
        let mixed = WalBatch {
            commit_ts: 12,
            recorded_at: batch().recorded_at,
            entries: vec![
                WalEntry::Put {
                    key: b"k".to_vec(),
                    value: b"old".to_vec(),
                },
                WalEntry::RangeDelete {
                    start: b"a".to_vec(),
                    end: b"z".to_vec(),
                },
                WalEntry::Put {
                    key: b"k".to_vec(),
                    value: b"new".to_vec(),
                },
                WalEntry::PointDelete { key: b"x".to_vec() },
            ],
        };
        let decoded =
            decode_v5_batch(&encode_v5_batch(&mixed, limits()).unwrap(), 0, limits()).unwrap();
        assert_eq!(
            decoded.batch.entries,
            vec![
                WalEntry::RangeDelete {
                    start: b"a".to_vec(),
                    end: b"z".to_vec(),
                },
                WalEntry::Put {
                    key: b"k".to_vec(),
                    value: b"new".to_vec(),
                },
                WalEntry::PointDelete { key: b"x".to_vec() },
            ]
        );
    }

    #[test]
    fn v5_decoder_rejects_noncanonical_duplicate_point_operations() {
        let mut duplicate = batch();
        duplicate.entries.insert(
            1,
            WalEntry::Put {
                key: b"a".to_vec(),
                value: b"latest".to_vec(),
            },
        );
        let encoded = encode_v5_batch_inner(&duplicate, limits()).unwrap();
        assert!(decode_v5_batch(&encoded, 0, limits()).is_err());
    }

    #[test]
    fn v5_codec_enforces_configured_field_and_batch_limits() {
        let encoded = encode_v5_batch(&batch(), limits()).unwrap();

        let mut key_limited = limits();
        key_limited.max_key_bytes = 0;
        assert!(decode_v5_batch(&encoded, 0, key_limited).is_err());
        assert!(encode_v5_batch(&batch(), key_limited).is_err());

        let mut value_limited = limits();
        value_limited.max_value_bytes = 2;
        assert!(decode_v5_batch(&encoded, 0, value_limited).is_err());
        assert!(encode_v5_batch(&batch(), value_limited).is_err());

        let mut batch_limited = limits();
        batch_limited.max_batch_data_bytes = 44;
        assert!(decode_v5_batch(&encoded, 0, batch_limited).is_err());
        assert!(encode_v5_batch(&batch(), batch_limited).is_err());

        let mut duplicate = batch();
        duplicate
            .entries
            .push(WalEntry::PointDelete { key: b"b".to_vec() });
        let mut entry_limited = limits();
        entry_limited.max_entry_count = duplicate.entries.len() - 1;
        assert!(encode_v5_batch(&duplicate, entry_limited).is_ok());

        let mut input_limited = limits();
        input_limited.max_input_entry_count = duplicate.entries.len() - 1;
        input_limited.max_entry_count = input_limited.max_input_entry_count;
        assert!(encode_v5_batch(&duplicate, input_limited).is_err());

        let mut inconsistent = limits();
        inconsistent.max_input_entry_count = inconsistent.max_entry_count - 1;
        assert!(encode_v5_batch(&batch(), inconsistent).is_err());
        assert!(decode_v5_batch(&encoded, 0, inconsistent).is_err());
    }

    #[test]
    fn v5_rejects_bad_crc_reserved_flags_and_trailing_data() {
        let encoded = encode_v5_batch(&batch(), limits()).unwrap();
        let mut bad_crc = encoded.clone();
        bad_crc[32] ^= 1;
        assert!(decode_v5_batch(&bad_crc, 0, limits()).is_err());

        let mut bad_reserved = encoded.clone();
        bad_reserved[39] = 1;
        assert!(decode_v5_batch(&bad_reserved, 0, limits()).is_err());

        let mut bad_flags = encoded;
        bad_flags[41] = 1;
        assert!(decode_v5_batch(&bad_flags, 0, limits()).is_err());

        let mut trailing_data = encode_v5_batch(&batch(), limits()).unwrap();
        trailing_data[20..24].copy_from_slice(&2_u32.to_be_bytes());
        let header_crc = crc32fast::hash(&trailing_data[..28]);
        trailing_data[32..36].copy_from_slice(&header_crc.to_be_bytes());
        assert!(decode_v5_batch(&trailing_data, 0, limits()).is_err());
    }

    #[test]
    fn v5_batch_rejects_malformed_fixed_fields_and_framing() {
        let valid = encode_v5_batch(&batch(), limits()).unwrap();
        assert!(decode_v5_batch(&valid[..39], 0, limits()).is_err());
        assert!(decode_v5_batch(&valid[..60], 0, limits()).is_err());
        assert!(decode_v5_batch(&valid, 1, limits()).is_err());

        for range in [0..8, 20..24, 24..28] {
            let mut corrupted = valid.clone();
            corrupted[range].fill(0);
            refresh_batch_crcs(&mut corrupted);
            assert!(decode_v5_batch(&corrupted, 0, limits()).is_err());
        }

        let mut invalid_nanos = valid.clone();
        invalid_nanos[16..20].copy_from_slice(&1_000_000_000_u32.to_be_bytes());
        refresh_batch_crcs(&mut invalid_nanos);
        assert!(decode_v5_batch(&invalid_nanos, 0, limits()).is_err());

        let mut unknown_kind = valid.clone();
        unknown_kind[40] = 0xff;
        refresh_batch_crcs(&mut unknown_kind);
        assert!(decode_v5_batch(&unknown_kind, 0, limits()).is_err());

        let mut truncated_payload = valid.clone();
        truncated_payload[42..46].copy_from_slice(&u32::MAX.to_be_bytes());
        refresh_batch_crcs(&mut truncated_payload);
        assert!(decode_v5_batch(&truncated_payload, 0, limits()).is_err());

        let mut nonzero_gap = valid;
        *nonzero_gap.last_mut().unwrap() = 1;
        assert!(decode_v5_batch(&nonzero_gap, 0, limits()).is_err());
    }

    #[test]
    fn v5_commit_time_digest_binds_epoch_and_segment() {
        let high_water = CommitTimeHighWater {
            archive_epoch_id: ArchiveEpochId([2; 16]),
            segment_id: SegmentId(7),
            commit_ts: 11,
            recorded_at: batch().recorded_at,
            entry_digest: [0; 32],
        };
        let digest = commit_time_entry_digest(high_water);
        assert_eq!(
            digest,
            [
                8, 82, 24, 218, 63, 209, 62, 92, 199, 147, 156, 205, 116, 94, 84, 242, 3, 242, 85,
                202, 205, 86, 154, 126, 237, 105, 17, 145, 27, 137, 55, 131,
            ]
        );
        let mut changed = high_water;
        changed.segment_id = SegmentId(8);
        assert_ne!(digest, commit_time_entry_digest(changed));
    }

    /// The encoder's output is a wire format, so its bytes are pinned rather than
    /// only round-tripped - a stricter check, because a round-trip only catches
    /// what the decoder disagrees with, and a change made consistently on both
    /// sides would survive it. Each digest below is of the bytes the
    /// implementation produced *before* it was changed to write straight into the
    /// ring buffer - taken from the same cases by the previous implementation - so
    /// this fails if the change altered any byte the encoder emits.
    ///
    /// The destination is poisoned before encoding, which is what gives the claim
    /// above its force. Encoding into a fresh `Vec` - as calling `encode_v5_batch`
    /// would - makes zeroing the reserved tail and the alignment padding
    /// unobservable, because those bytes are already zero: deleting either
    /// `fill(0)` would leave every digest below matching while the ring buffer,
    /// which is pooled and still holds the previous batch's bytes, went on to
    /// expose stale data past the logical end. A dirty destination is the only
    /// version of this test that can fail for the bug it was written for.
    #[test]
    fn encoder_bytes_are_unchanged_by_writing_into_the_buffer() {
        let limits = LIVE_WAL_V5_LIMITS;
        let cases: Vec<(&str, WalBatch, usize, &str)> = vec![
            (
                "single put",
                WalBatch {
                    commit_ts: 7,
                    recorded_at: RecordedAt {
                        secs: 1_700_000_000,
                        nanos: 42,
                    },
                    entries: vec![WalEntry::Put {
                        key: b"key-0001".to_vec(),
                        value: b"value-0001".to_vec(),
                    }],
                },
                4096,
                "1fe36041eca9fdf1a1954324c533b6cc59942ceac98fe98e26c2a14295f23882",
            ),
            (
                "point delete",
                WalBatch {
                    commit_ts: 8,
                    recorded_at: RecordedAt {
                        secs: 1_700_000_001,
                        nanos: 43,
                    },
                    entries: vec![WalEntry::PointDelete {
                        key: b"gone".to_vec(),
                    }],
                },
                4096,
                "56cd008b5f0c7af10279ae4985fed477ff5548bde8b64741681bb58bd46e418d",
            ),
            (
                "range delete",
                WalBatch {
                    commit_ts: 9,
                    recorded_at: RecordedAt {
                        secs: 1_700_000_002,
                        nanos: 44,
                    },
                    entries: vec![WalEntry::RangeDelete {
                        start: b"a".to_vec(),
                        end: b"z".to_vec(),
                    }],
                },
                4096,
                "8c5c5683ef4fbc868508b433f772dfdc5516ce26610d39f4549646720d08bb24",
            ),
            (
                "duplicate keys canonicalise",
                WalBatch {
                    commit_ts: 10,
                    recorded_at: RecordedAt {
                        secs: 1_700_000_003,
                        nanos: 45,
                    },
                    entries: vec![
                        WalEntry::Put {
                            key: b"k".to_vec(),
                            value: b"first".to_vec(),
                        },
                        WalEntry::Put {
                            key: b"other".to_vec(),
                            value: b"x".to_vec(),
                        },
                        WalEntry::Put {
                            key: b"k".to_vec(),
                            value: b"last".to_vec(),
                        },
                    ],
                },
                4096,
                "1ef8bc680066b75b98a4c061f28b8f0a8a9d3330477d4bf2413b385091841b5e",
            ),
            (
                "full padding boundary",
                WalBatch {
                    commit_ts: 11,
                    recorded_at: RecordedAt {
                        secs: 1_700_000_004,
                        nanos: 46,
                    },
                    entries: vec![WalEntry::Put {
                        key: vec![b'k'; 60],
                        value: vec![b'v'; 4000],
                    }],
                },
                8192,
                "a3a444a6469c0708f1db2cc2a53072cb5edaed4adeb09ba77f4b72e25db9c40b",
            ),
        ];
        for (name, batch, expected_len, expected_digest) in cases {
            let canonical = canonical_batch(&batch, limits).unwrap();
            let mut encoded = vec![0xAA_u8; expected_len];
            let written = encode_v5_batch_into(&canonical, limits, &mut encoded).unwrap();
            assert_eq!(written, expected_len, "{name}: encoded length");
            assert_eq!(
                format!("{:x}", Sha256::digest(&encoded)),
                expected_digest,
                "{name}: encoded bytes changed"
            );
        }
    }
}
