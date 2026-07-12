pub(crate) mod bloom;
mod builder;
mod iterator;

use std::os::unix::io::AsRawFd;
use std::sync::OnceLock;
use std::{fs::File, mem, ops::Bound, path::Path, sync::Arc};

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use self::bloom::Bloom;
use crate::{
    block::Block,
    iterators::StorageIterator,
    key::{Key, KeyBytes, KeySlice, TS_ENABLED},
    lsm_storage::{BlockCache, CacheAdmission},
};

const SIZE_OF_U16: usize = mem::size_of::<u16>();
const SIZE_OF_U32: usize = mem::size_of::<u32>();
const SIZE_OF_U64: usize = mem::size_of::<u64>();

/// Magic number for MVCC-format SSTs: "MVCC" in ASCII.
const SST_MVCC_MAGIC: u32 = 0x4D56_4343;
/// Footer version for MVCC-format SSTs (no prefix bloom).
const SST_FOOTER_VERSION_V2: u8 = 2;
/// Footer version for SSTs with prefix bloom filters.
const SST_FOOTER_VERSION_V3: u8 = 3;
/// Footer version for SSTs with range-tombstone blocks.
const SST_FOOTER_VERSION_V4: u8 = 4;
/// Footer version for MVCC-format SSTs with stable persisted whole-key bloom hashing.
const SST_FOOTER_VERSION_V5: u8 = 5;
/// Footer version for SSTs with stable whole-key + prefix bloom hashing.
const SST_FOOTER_VERSION_V6: u8 = 6;
/// Footer version for SSTs with stable whole-key + prefix bloom hashing and range tombstones.
const SST_FOOTER_VERSION_V7: u8 = 7;
/// Footer version for SSTs with TTL metadata.
const SST_FOOTER_VERSION_V8: u8 = 8;
/// Footer version for SSTs with TTL entry counts (v8 + 16 bytes).
const SST_FOOTER_VERSION_V9: u8 = 9;
/// Size of the MVCC footer extension: max_ts (8) + magic (4) + version (1) = 13 bytes.
const MVCC_FOOTER_EXTRA: u64 = (SIZE_OF_U64 + SIZE_OF_U32 + 1) as u64;

/// A single prefix bloom filter for a specific prefix length.
#[derive(Debug)]
pub(crate) struct PrefixBloom {
    pub(crate) prefix_len: usize,
    pub(crate) bloom: Bloom,
}

/// Collection of prefix bloom filters for an SST, sorted by prefix_len ascending.
#[derive(Debug)]
pub(crate) struct PrefixBloomSet {
    filters: Vec<PrefixBloom>,
}

impl PrefixBloomSet {
    /// Find the best (longest) filter whose `prefix_len <= query_len`.
    pub(crate) fn best_filter_for(&self, query_len: usize) -> Option<&PrefixBloom> {
        self.filters
            .iter()
            .rev()
            .find(|f| f.prefix_len <= query_len)
    }
}

/// TTL metadata for an SST.
#[derive(Clone, Copy, Debug, Default)]
pub struct SsTableTtlMetadata {
    /// The minimum `expire_at_secs` among all TTL entries in this SST.
    /// `u64::MAX` if the SST contains no TTL entries.
    pub min_ttl_expire_ts: u64,
    /// The maximum `expire_at_secs` among all TTL entries in this SST.
    /// 0 if the SST contains no TTL entries.
    pub max_ttl_expire_ts: u64,
    /// Whether this SST contains any non-TTL entries (Inline, ValuePointer,
    /// Tombstone). If true, the SST cannot be wholesale-dropped even if all
    /// TTL entries have expired, because non-TTL entries never expire.
    pub has_non_ttl_entries: bool,
    /// Number of TTL entries (TtlInline + TtlValuePointer) in this SST.
    pub ttl_entry_count: u64,
    /// Total number of entries in this SST.
    pub total_entry_count: u64,
}

impl SsTableTtlMetadata {
    /// Sentinel values for SSTs with no TTL entries.
    pub const NONE: Self = Self {
        min_ttl_expire_ts: u64::MAX,
        max_ttl_expire_ts: 0,
        has_non_ttl_entries: true,
        ttl_entry_count: 0,
        total_entry_count: 0,
    };
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    fn estimated_size(block_meta: &[BlockMeta]) -> usize {
        let mut estimated_size = std::mem::size_of::<u32>();
        for meta in block_meta {
            // The size of offset
            estimated_size += std::mem::size_of::<u32>();

            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.first_key.len();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.last_key.len();
            // Offset pointer in the offsets array (one u32 per entry)
            estimated_size += std::mem::size_of::<u32>();
        }
        // number of entries
        estimated_size += std::mem::size_of::<u32>();

        estimated_size
    }

    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut offsets = vec![];
        buf.reserve(BlockMeta::estimated_size(block_meta));

        for meta in block_meta {
            offsets.push(buf.len() as u32);

            buf.put_u16(meta.first_key.len() as u16);
            buf.put(meta.first_key.raw_ref());

            buf.put_u16(meta.last_key.len() as u16);
            buf.put(meta.last_key.raw_ref());

            buf.put_u32(meta.offset as u32);
        }

        for o in &offsets {
            buf.put_u32(*o);
        }
        buf.put_u32(offsets.len() as u32);
    }

    /// Decode block meta from a buffer.
    /// Returns an error if the metadata is malformed (corrupt on-disk data).
    pub fn decode_block_meta(data: &[u8]) -> Result<Vec<BlockMeta>> {
        anyhow::ensure!(
            data.len() >= SIZE_OF_U32,
            "SST block metadata too small: {} bytes",
            data.len()
        );
        let num_of_elements = (&data[data.len() - SIZE_OF_U32..]).get_u32() as usize;
        let offsets_len = num_of_elements
            .checked_mul(SIZE_OF_U32)
            .ok_or_else(|| anyhow::anyhow!("SST block metadata offset table size overflow"))?;
        let offset = data
            .len()
            .checked_sub(SIZE_OF_U32)
            .and_then(|len| len.checked_sub(offsets_len))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "SST block metadata offset table out of bounds: entries={}, len={}",
                    num_of_elements,
                    data.len()
                )
            })?;
        let mut datas = &data[0..offset];

        // Guard against corrupt SST headers: each block meta entry is at
        // minimum 8 bytes (two u16 key lengths + one u32 offset), so the
        // entry count cannot exceed the payload divided by 8.
        const MIN_ENTRY_SIZE: usize = SIZE_OF_U16 + SIZE_OF_U16 + SIZE_OF_U32;
        anyhow::ensure!(
            num_of_elements <= offset / MIN_ENTRY_SIZE,
            "SST block metadata count {} exceeds maximum possible entries for payload size {}",
            num_of_elements,
            offset
        );

        let mut ret = Vec::with_capacity(num_of_elements);
        for _ in 0..num_of_elements {
            anyhow::ensure!(
                datas.len() >= 2,
                "SST block metadata missing first key length"
            );
            let first_key_len = datas.get_u16() as usize;
            anyhow::ensure!(
                datas.len() >= first_key_len,
                "SST block metadata first key out of bounds"
            );
            let first_key = &datas[..first_key_len];
            datas.advance(first_key_len);

            anyhow::ensure!(
                datas.len() >= 2,
                "SST block metadata missing last key length"
            );
            let last_key_len = datas.get_u16() as usize;
            anyhow::ensure!(
                datas.len() >= last_key_len,
                "SST block metadata last key out of bounds"
            );
            let last_key = &datas[..last_key_len];
            datas.advance(last_key_len);

            anyhow::ensure!(
                datas.len() >= SIZE_OF_U32,
                "SST block metadata missing offset"
            );
            let offset = datas.get_u32() as usize;

            ret.push(BlockMeta {
                offset,
                first_key: Key::from_vec(first_key.to_vec()).into_key_bytes(),
                last_key: Key::from_vec(last_key.to_vec()).into_key_bytes(),
            })
        }

        Ok(ret)
    }
}

/// A file object.
#[derive(Debug)]
pub struct FileObject(Option<File>, u64);

/// Per-SST MVCC-GC candidate stats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SsTableMvccGcStats {
    /// SST id.
    pub sst_id: usize,
    /// SST level when reported through the engine stats API.
    pub level: Option<usize>,
    /// Maximum timestamp in the SST.
    pub max_ts: u64,
    /// Minimum TTL expiration timestamp in the SST.
    pub min_ttl_expire_ts: u64,
    /// Maximum TTL expiration timestamp in the SST.
    pub max_ttl_expire_ts: u64,
    /// Whether the SST contains non-TTL entries.
    pub has_non_ttl_entries: bool,
    /// Number of TTL entries in the SST.
    pub ttl_entry_count: u64,
    /// Total number of entries in the SST.
    pub total_entry_count: u64,
    /// Number of point tombstones in the SST.
    pub point_tombstone_count: u64,
    /// Number of range-tombstone fragments in the SST.
    pub range_tombstone_fragment_count: u64,
    /// Approximate bytes of range-tombstone metadata in the SST.
    pub range_tombstone_metadata_bytes: u64,
    /// Estimated redundant historical versions in the SST.
    pub redundant_version_count: u64,
}

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .expect("FileObject::read called after file was dropped")
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    pub fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.0
            .as_ref()
            .expect("FileObject::as_raw_fd called after file was dropped")
            .as_raw_fd()
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    /// First point key. `None` for range-only SSTs.
    first_key: Option<KeyBytes>,
    /// Last point key. `None` for range-only SSTs.
    last_key: Option<KeyBytes>,
    pub(crate) bloom: Option<Bloom>,
    /// Whether persisted whole-key/prefix bloom hashes are the stable
    /// cross-process variant introduced in footer versions v5+.
    stable_bloom_hash: bool,
    /// The maximum timestamp stored in this SST, implemented in MVCC.
    max_ts: u64,
    /// Prefix bloom filters for prefix scan pruning. Present in v3, v4, v6, and v7 SSTs.
    pub(crate) prefix_blooms: Option<PrefixBloomSet>,
    /// Cached range-tombstone fragments. Present in v4 and v7 SSTs with range tombstones.
    range_tombstones: Option<Arc<[crate::range_tombstone::RangeTombstoneFragment]>>,
    /// TTL metadata for this SST. Populated from footer v8/v9.
    /// Defaults to sentinel values for v2–v7 SSTs.
    /// TODO(ttl-read-path): consume this field for read-path TTL filtering
    /// and wholesale-drop optimizations in compaction.
    #[allow(dead_code)]
    pub(crate) ttl_metadata: SsTableTtlMetadata,
    /// Last block index hint for sorted batch lookups. When consecutive
    /// sorted keys land in the same block, this avoids a full binary search.
    last_block_hint: std::sync::atomic::AtomicUsize,
    /// Cached MVCC GC stats computed on first access. An SST's stats cannot
    /// change after construction, so caching avoids repeated full SST scans
    /// in the background compaction loop.
    mvcc_gc_stats_cache: OnceLock<SsTableMvccGcStats>,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // Guard against empty or truncated SST files.
        anyhow::ensure!(
            file.size() >= SIZE_OF_U32 as u64,
            "SST file too small: {} bytes",
            file.size()
        );
        // Read the tail region in a single I/O call. v3/v6 footers need 21 bytes
        // (bloom_offset:4 + prefix_bloom_offset:4 + max_ts:8 + magic:4 + version:1),
        // v2/v5 need 17 bytes (bloom_offset:4 + max_ts:8 + magic:4 + version:1).
        // Read 21 to cover both cases; v4/v7 (25 bytes) get a re-read below.
        let v3_tail_size = MVCC_FOOTER_EXTRA as usize + SIZE_OF_U32 + SIZE_OF_U32;
        let tail_start = file.size().saturating_sub(v3_tail_size as u64);
        let tail = file.read(tail_start, file.size() - tail_start)?;

        // Detect MVCC footer by checking magic AND version at their expected
        // positions. Versions 2-4 use the legacy persisted bloom hash;
        // versions 5-7 use the stable cross-process persisted bloom hash.
        let version = tail[tail.len() - 1];
        let has_mvcc_magic = tail.len() >= (MVCC_FOOTER_EXTRA as usize + SIZE_OF_U32)
            && (&tail[tail.len() - 5..tail.len() - 1]).get_u32() == SST_MVCC_MAGIC;
        if has_mvcc_magic
            && version != SST_FOOTER_VERSION_V2
            && version != SST_FOOTER_VERSION_V3
            && version != SST_FOOTER_VERSION_V4
            && version != SST_FOOTER_VERSION_V5
            && version != SST_FOOTER_VERSION_V6
            && version != SST_FOOTER_VERSION_V7
            && version != SST_FOOTER_VERSION_V8
            && version != SST_FOOTER_VERSION_V9
        {
            anyhow::bail!(
                "unsupported MVCC footer version: {} (expected v2–v9)",
                version,
            );
        }
        let is_mvcc = has_mvcc_magic
            && (version == SST_FOOTER_VERSION_V2
                || version == SST_FOOTER_VERSION_V3
                || version == SST_FOOTER_VERSION_V4
                || version == SST_FOOTER_VERSION_V5
                || version == SST_FOOTER_VERSION_V6
                || version == SST_FOOTER_VERSION_V7
                || version == SST_FOOTER_VERSION_V8
                || version == SST_FOOTER_VERSION_V9);
        let stable_bloom_hash = matches!(
            version,
            SST_FOOTER_VERSION_V5
                | SST_FOOTER_VERSION_V6
                | SST_FOOTER_VERSION_V7
                | SST_FOOTER_VERSION_V8
                | SST_FOOTER_VERSION_V9
        );

        // v4/v7 footer is 25 bytes, v8 footer is 42 bytes, v9 is 58 bytes:
        // v4/v7: [bloom_offset:4][prefix_bloom_offset:4][range_tombstone_offset:4]
        //        [max_ts:8][magic:4][version:1] = 25 bytes
        // v8:    [bloom_offset:4][prefix_bloom_offset:4][range_tombstone_offset:4]
        //        [min_ttl_expire_ts:8][max_ttl_expire_ts:8][has_non_ttl:1]
        //        [max_ts:8][magic:4][version:1] = 42 bytes
        // v9:    v8 + [ttl_entry_count:8][total_entry_count:8] = 58 bytes
        let (bloom_offset_base, mut max_ts, bloom_offset, prefix_bloom_set, v4_rt_off, ttl_meta) =
            if is_mvcc
                && matches!(
                    version,
                    SST_FOOTER_VERSION_V4
                        | SST_FOOTER_VERSION_V7
                        | SST_FOOTER_VERSION_V8
                        | SST_FOOTER_VERSION_V9
                )
            {
                let is_v8 = version == SST_FOOTER_VERSION_V8 || version == SST_FOOTER_VERSION_V9;
                // v8: 42 bytes (3×u32 + 2×u64 + 1×u8 + max_ts:8 + magic:4 + version:1)
                // v9: 58 bytes (v8 + 2×u64: ttl_entry_count + total_entry_count)
                let tail_size = if is_v8 {
                    let base =
                        SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U64 + 1;
                    // v9 adds 16 bytes: ttl_entry_count:8 + total_entry_count:8
                    if version == SST_FOOTER_VERSION_V9 {
                        base + SIZE_OF_U64 + SIZE_OF_U64 + MVCC_FOOTER_EXTRA as usize
                    } else {
                        base + MVCC_FOOTER_EXTRA as usize
                    }
                } else {
                    SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U32 + MVCC_FOOTER_EXTRA as usize
                };
                let tail = file.read(
                    file.size().saturating_sub(tail_size as u64),
                    tail_size as u64,
                )?;
                anyhow::ensure!(
                    tail.len() >= tail_size,
                    "SST footer read returned fewer bytes than expected: got {}, expected {}",
                    tail.len(),
                    tail_size
                );
                let bloom_off = (&tail[0..4]).get_u32() as u64;
                let prefix_bloom_off = (&tail[4..8]).get_u32() as u64;
                let rt_off = (&tail[8..12]).get_u32() as u64;
                let (max_ts_val, ttl_metadata) = if is_v8 {
                    // v8 layout (42 bytes): [0..4]bloom [4..8]prefix [8..12]rt [12..20]min_ttl
                    //   [20..28]max_ttl [28]has_non_ttl [29..37]max_ts [37..41]magic [41]version
                    // v9 layout (58 bytes): [0..28] same as v8, then
                    //   [29..37]ttl_entry_count [37..45]total_entry_count
                    //   [45..53]max_ts [53..57]magic [57]version
                    // v9 shifts max_ts/magic/version by 16 bytes (the two count fields).
                    let min_ttl = (&tail[12..20]).get_u64();
                    let max_ttl = (&tail[20..28]).get_u64();
                    let has_non_ttl = tail[28] != 0;
                    let max_ts_off: usize = if version == SST_FOOTER_VERSION_V9 {
                        45
                    } else {
                        29
                    };
                    let max_ts_val = (&tail[max_ts_off..max_ts_off + 8]).get_u64();
                    let (ttl_count, total_count) = if version == SST_FOOTER_VERSION_V9 {
                        ((&tail[29..37]).get_u64(), (&tail[37..45]).get_u64())
                    } else {
                        (0, 0)
                    };
                    (
                        max_ts_val,
                        SsTableTtlMetadata {
                            min_ttl_expire_ts: min_ttl,
                            max_ttl_expire_ts: max_ttl,
                            has_non_ttl_entries: has_non_ttl,
                            ttl_entry_count: ttl_count,
                            total_entry_count: total_count,
                        },
                    )
                } else {
                    let max_ts_val = (&tail[12..20]).get_u64();
                    (max_ts_val, SsTableTtlMetadata::NONE)
                };
                let footer_start = file.size() - tail_size as u64;
                let prefix_set = if prefix_bloom_off > 0 {
                    let section_end = if rt_off > 0 { rt_off } else { footer_start };
                    Self::decode_prefix_blooms(&file, prefix_bloom_off, section_end)?
                } else {
                    None
                };
                let base = if prefix_bloom_off > 0 {
                    prefix_bloom_off
                } else if rt_off > 0 {
                    rt_off
                } else {
                    footer_start
                };
                (
                    base,
                    max_ts_val,
                    bloom_off,
                    prefix_set,
                    Some(rt_off),
                    ttl_metadata,
                )
            } else if is_mvcc && matches!(version, SST_FOOTER_VERSION_V3 | SST_FOOTER_VERSION_V6) {
                // v3/v6 file layout (from end of file):
                //   [version:1][magic:4][max_ts:8][prefix_bloom_offset:4]
                //   [prefix_bloom_section (variable size)]
                //   [bloom_offset:4]
                //
                // The 21-byte tail read captures the fixed 17-byte footer
                // extension plus 4 bytes of the preceding data. The
                // prefix_bloom_offset is at a fixed position within the tail.
                let footer_start = file.size() - MVCC_FOOTER_EXTRA;
                let footer = &tail[tail.len() - MVCC_FOOTER_EXTRA as usize..];
                let max_ts = (&footer[0..8]).get_u64();
                // prefix_bloom_offset is at file position (footer_start - 4),
                // which is tail[4..8] in the 21-byte read.
                let prefix_bloom_off = (&tail[tail.len() - MVCC_FOOTER_EXTRA as usize - SIZE_OF_U32
                    ..tail.len() - MVCC_FOOTER_EXTRA as usize])
                    .get_u32() as u64;
                // bloom_offset is at the 4 bytes immediately before the prefix
                // bloom section. Read it from the file using prefix_bloom_off
                // as an anchor (the 21-byte tail does NOT capture bloom_offset
                // when the prefix bloom section is non-empty).
                anyhow::ensure!(
                    prefix_bloom_off >= SIZE_OF_U32 as u64,
                    "SST prefix_bloom_offset out of bounds: {}",
                    prefix_bloom_off
                );
                let section_end = footer_start - SIZE_OF_U32 as u64;
                anyhow::ensure!(
                    prefix_bloom_off < section_end,
                    "SST prefix_bloom_offset ({}) >= section_end ({})",
                    prefix_bloom_off,
                    section_end
                );
                let bloom_off = file
                    .read(prefix_bloom_off - SIZE_OF_U32 as u64, SIZE_OF_U32 as u64)?
                    .as_slice()
                    .get_u32() as u64;

                // Decode prefix bloom section.
                let prefix_blooms =
                    Self::decode_prefix_blooms(&file, prefix_bloom_off, section_end)?;

                // bloom_offset_base = start of prefix bloom section (bloom data
                // ends right before it).
                (
                    prefix_bloom_off,
                    max_ts,
                    bloom_off,
                    prefix_blooms,
                    None,
                    SsTableTtlMetadata::NONE,
                )
            } else if is_mvcc {
                // v2 tail layout: [bloom_offset: u32][max_ts: u64][magic: u32][version: u8]
                //                 bytes 0..4    bytes 4..12   bytes 12..16  byte 16
                let footer = &tail[tail.len() - MVCC_FOOTER_EXTRA as usize..];
                let max_ts = (&footer[0..8]).get_u64();
                let footer_start = file.size() - MVCC_FOOTER_EXTRA;
                let bloom_off = (&tail[tail.len() - MVCC_FOOTER_EXTRA as usize - SIZE_OF_U32
                    ..tail.len() - MVCC_FOOTER_EXTRA as usize])
                    .get_u32() as u64;
                (
                    footer_start,
                    max_ts,
                    bloom_off,
                    None,
                    None,
                    SsTableTtlMetadata::NONE,
                )
            } else {
                // Legacy footer: last 4 bytes are bloom_offset.
                let bloom_off = (&tail[tail.len() - SIZE_OF_U32..]).get_u32() as u64;
                (
                    file.size(),
                    0,
                    bloom_off,
                    None,
                    None,
                    SsTableTtlMetadata::NONE,
                )
            };
        anyhow::ensure!(
            bloom_offset >= SIZE_OF_U32 as u64
                && bloom_offset
                    .checked_add(SIZE_OF_U32 as u64)
                    .is_some_and(|sum| sum <= bloom_offset_base),
            "SST bloom offset out of bounds: {}",
            bloom_offset
        );
        let bloom = bloom::Bloom::decode(
            file.read(
                bloom_offset,
                bloom_offset_base - bloom_offset - SIZE_OF_U32 as u64,
            )?
            .as_slice(),
        )?;

        let meta_offset = file
            .read(bloom_offset - SIZE_OF_U32 as u64, SIZE_OF_U32 as u64)?
            .as_slice()
            .get_u32() as u64;
        anyhow::ensure!(
            meta_offset <= bloom_offset - SIZE_OF_U32 as u64,
            "SST meta block out of bounds: meta_offset={}, bloom_offset={}",
            meta_offset,
            bloom_offset
        );
        let meta_len = bloom_offset - SIZE_OF_U32 as u64 - meta_offset;
        let block_meta = if meta_len == 0 {
            Vec::new()
        } else {
            anyhow::ensure!(
                meta_len >= SIZE_OF_U32 as u64,
                "SST meta block too small: meta_offset={}, bloom_offset={}",
                meta_offset,
                bloom_offset
            );
            BlockMeta::decode_block_meta(&file.read(meta_offset, meta_len)?)?
        };
        // Decode range-tombstone block (v4 only).
        let range_tombstones = if let Some(rt_off) = v4_rt_off {
            if rt_off > 0 {
                // Range-tombstone block extends from rt_off to the start of
                // the 25-byte v4 footer.
                let footer_start = file.size() - 25;
                anyhow::ensure!(
                    rt_off <= footer_start,
                    "range tombstone offset ({rt_off}) exceeds footer start ({footer_start})"
                );
                let rt_data = file.read(rt_off, footer_start - rt_off)?;
                let frags = crate::range_tombstone::RangeTombstoneFragment::decode_block(
                    rt_data.as_slice(),
                )?;
                // Update max_ts from range-tombstone timestamps.
                for frag in &frags {
                    for &ts in &frag.covering_ts {
                        if ts > max_ts {
                            max_ts = ts;
                        }
                    }
                }
                Some(Arc::from(frags))
            } else {
                None
            }
        } else {
            None
        };

        // Decode block metadata. Range-only SSTs may have empty block_meta.
        let (first_key, last_key) = if block_meta.is_empty() {
            anyhow::ensure!(
                range_tombstones.is_some(),
                "SST has no block metadata and no range-tombstone block"
            );
            (None, None)
        } else {
            // Reject footer-less MVCC SSTs: scanning only first_key/last_key per
            // block cannot recover the true max_ts (a middle key may carry a higher
            // timestamp). Underestimating max_ts would allow commit timestamp reuse
            // after restart, violating MVCC safety. Require the v2 footer.
            // Keys with ts=0 are the default for non-MVCC usage and are not a concern.
            if max_ts == 0 {
                let looks_like_mvcc_key = |raw: &[u8]| {
                    crate::key::decode_user_key(raw).is_some()
                        && crate::key::extract_ts(raw).is_some_and(|ts| ts > 0)
                };
                for meta in &block_meta {
                    if looks_like_mvcc_key(meta.first_key.raw_ref())
                        || looks_like_mvcc_key(meta.last_key.raw_ref())
                    {
                        anyhow::bail!(
                            "SST contains MVCC-encoded keys but has no v2 footer; \
                             re-flush or compact to add the footer before upgrading"
                        );
                    }
                }
            }
            (
                Some(block_meta[0].first_key.clone()),
                Some(block_meta[block_meta.len() - 1].last_key.clone()),
            )
        };
        Ok(Self {
            file,
            block_meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            stable_bloom_hash,
            max_ts,
            prefix_blooms: prefix_bloom_set,
            range_tombstones,
            ttl_metadata: ttl_meta,
            last_block_hint: std::sync::atomic::AtomicUsize::new(0),
            mvcc_gc_stats_cache: OnceLock::new(),
        })
    }

    /// Decode the prefix bloom section from a v3 SST.
    ///
    /// The section is at `[prefix_bloom_offset, prefix_bloom_offset_field)`.
    ///
    /// Format:
    /// ```text
    /// u16 filter_count
    /// repeated filter_count times:
    ///     u16 prefix_len
    ///     u32 filter_offset
    ///     u32 filter_len
    /// filter bytes...
    /// ```
    fn decode_prefix_blooms(
        file: &FileObject,
        prefix_bloom_offset: u64,
        prefix_bloom_offset_field: u64,
    ) -> Result<Option<PrefixBloomSet>> {
        anyhow::ensure!(
            prefix_bloom_offset < prefix_bloom_offset_field,
            "SST prefix bloom section has invalid bounds: offset={}, field={}",
            prefix_bloom_offset,
            prefix_bloom_offset_field
        );
        let section_size = prefix_bloom_offset_field - prefix_bloom_offset;
        anyhow::ensure!(
            section_size >= 2,
            "SST prefix bloom section too small: {} bytes",
            section_size
        );
        let section_size_usize = usize::try_from(section_size).map_err(|_| {
            anyhow::anyhow!(
                "SST prefix bloom section too large for platform: {} bytes",
                section_size
            )
        })?;
        let section = file.read(prefix_bloom_offset, section_size)?;
        let filter_count = (&section[0..2]).get_u16() as usize;
        if filter_count == 0 {
            // Empty prefix bloom section: no prefix-bloom pruning available.
            // This is the normal representation for "no filters" and also
            // tolerates legacy SSTs whose prefix bloom section may have been
            // zeroed out after a hash-function migration.
            return Ok(None);
        }
        let table_len = 2 + filter_count * (2 + 4 + 4); // u16 + u32 + u32 per entry
        anyhow::ensure!(
            section_size_usize >= table_len,
            "SST prefix bloom section too small for {} filters: need {} bytes, have {}",
            filter_count,
            table_len,
            section_size
        );
        let filter_bytes_start = table_len;
        let filter_bytes_len = section_size_usize - filter_bytes_start;
        let mut filters = Vec::with_capacity(filter_count);
        let mut prev_prefix_len = 0usize;
        let mut prev_end = 0usize;
        for i in 0..filter_count {
            let entry_start = 2 + i * 10;
            let prefix_len = (&section[entry_start..entry_start + 2]).get_u16() as usize;
            let filter_offset = (&section[entry_start + 2..entry_start + 6]).get_u32() as usize;
            let filter_len = (&section[entry_start + 6..entry_start + 10]).get_u32() as usize;

            // Validate: prefix_len strictly increasing, within cap.
            anyhow::ensure!(
                prefix_len > 0 && prefix_len <= 64,
                "SST prefix bloom filter[{}]: invalid prefix_len={}",
                i,
                prefix_len
            );
            if i > 0 {
                anyhow::ensure!(
                    prefix_len > prev_prefix_len,
                    "SST prefix bloom filter[{}]: prefix_len={} not strictly increasing after {}",
                    i,
                    prefix_len,
                    prev_prefix_len
                );
            }
            // Validate: filter_offset + filter_len within filter_bytes area.
            let filter_end = filter_offset.checked_add(filter_len).ok_or_else(|| {
                anyhow::anyhow!("SST prefix bloom filter[{}]: offset overflow", i)
            })?;
            anyhow::ensure!(
                filter_end <= filter_bytes_len,
                "SST prefix bloom filter[{}]: extends beyond section ({} + {} > {})",
                i,
                filter_offset,
                filter_len,
                filter_bytes_len
            );
            // Validate: filter_len >= 2 (at least one filter byte + k byte).
            anyhow::ensure!(
                filter_len >= 2,
                "SST prefix bloom filter[{}]: filter_len={} too small",
                i,
                filter_len
            );
            // Validate: non-overlapping, sorted by offset.
            if i > 0 {
                anyhow::ensure!(
                    filter_offset >= prev_end,
                    "SST prefix bloom filter[{}]: overlaps with previous filter",
                    i
                );
            }
            // Validate: first filter starts at 0, last filter ends at filter_bytes_len.
            if i == 0 {
                anyhow::ensure!(
                    filter_offset == 0,
                    "SST prefix bloom filter[0]: offset must be 0, got {}",
                    filter_offset
                );
            }
            if i == filter_count - 1 {
                anyhow::ensure!(
                    filter_end == filter_bytes_len,
                    "SST prefix bloom filter[{}]: must end at section end ({} != {})",
                    i,
                    filter_end,
                    filter_bytes_len
                );
            }

            let filter_data = &section[filter_bytes_start + filter_offset
                ..filter_bytes_start + filter_offset + filter_len];
            let bloom = bloom::Bloom::decode(filter_data)?;
            filters.push(PrefixBloom { prefix_len, bloom });
            prev_prefix_len = prefix_len;
            prev_end = filter_end;
        }
        Ok(Some(PrefixBloomSet { filters }))
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key: Some(first_key),
            last_key: Some(last_key),
            bloom: None,
            stable_bloom_hash: false,
            max_ts: 0,
            prefix_blooms: None,
            range_tombstones: None,
            ttl_metadata: SsTableTtlMetadata::NONE,
            last_block_hint: std::sync::atomic::AtomicUsize::new(0),
            mvcc_gc_stats_cache: OnceLock::new(),
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let lo = self.block_meta[block_idx].offset as u64;
        let hi = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset) as u64;

        anyhow::ensure!(
            hi >= lo,
            "SST block {} out of bounds: lo={}, hi={}, block_meta_offset={}",
            block_idx,
            lo,
            hi,
            self.block_meta_offset,
        );
        let data = self.file.read(lo, hi - lo)?;
        let ret = Block::decode_from_vec(data)?;

        Ok(Arc::new(ret))
    }

    /// Read a block from disk, with block cache. (Day 4)
    ///
    /// Uses [`CacheAdmission::Force`] — always inserts on miss.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        self.read_block_cached_with_admission(block_idx, CacheAdmission::Force)
    }

    /// Hint to the kernel that `block_idx` will be needed soon, so it can
    /// start reading the block into the page cache in the background.
    /// Uses `posix_fadvise(POSIX_FADV_WILLNEED)`.  No-op if `block_idx`
    /// is out of range.
    pub fn prefetch_block(&self, block_idx: usize) {
        if block_idx >= self.block_meta.len() {
            return;
        }
        let lo = self.block_meta[block_idx].offset;
        let hi = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        if hi <= lo {
            return;
        }
        let fd = self.file.as_raw_fd();
        unsafe {
            libc::posix_fadvise(
                fd,
                lo as libc::off_t,
                (hi - lo) as libc::off_t,
                libc::POSIX_FADV_WILLNEED,
            );
        }
    }

    /// Read a block from disk with configurable cache admission policy.
    ///
    /// Like [`read_block_cached`](Self::read_block_cached) but the caller
    /// controls whether a miss should insert into the cache.
    pub fn read_block_cached_with_admission(
        &self,
        block_idx: usize,
        admission: crate::lsm_storage::CacheAdmission,
    ) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            return block_cache.try_get_with_admission(self.id, block_idx, admission, || {
                self.read_block(block_idx)
            });
        }

        self.read_block(block_idx)
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        if self.block_meta.is_empty() {
            return 0;
        }
        let mut lo = 0;
        let mut hi = self.block_meta.len() - 1;
        // For MVCC: track the earliest block whose user-key range contains the
        // seek key. Newer versions have smaller inverted_ts and sort first, so
        // the earliest matching block contains the newest version.
        let mut earliest_match: Option<usize> = None;

        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let first = self.block_meta[mid].first_key.as_key_slice();
            let last = self.block_meta[mid].last_key.as_key_slice();

            if TS_ENABLED {
                // Compare decoded user keys for MVCC. The search key uses
                // ts=u64::MAX so byte-order comparison against block boundaries
                // (which have real timestamps) would be wrong.
                let seek_uk = key.encoded_user_key();
                let first_uk = first.encoded_user_key();
                let last_uk = last.encoded_user_key();
                if seek_uk >= first_uk && seek_uk <= last_uk {
                    earliest_match = Some(mid);
                    // Continue searching left for an earlier block with the same
                    // user key range (may contain a newer version).
                    if mid == 0 {
                        return 0;
                    }
                    hi = mid - 1;
                } else if seek_uk < first_uk {
                    if mid == 0 {
                        return earliest_match.unwrap_or(0);
                    }
                    hi = mid - 1;
                } else {
                    if mid == self.block_meta.len() - 1 {
                        return earliest_match.unwrap_or(self.block_meta.len() - 1);
                    }
                    lo = mid + 1;
                }
            } else {
                if key >= first && key <= last {
                    return mid;
                }
                if key < first {
                    if mid == 0 {
                        return 0;
                    }
                    hi = mid - 1;
                } else {
                    if mid == self.block_meta.len() - 1 {
                        return self.block_meta.len() - 1;
                    }
                    lo = mid + 1;
                }
            }
        }
        // lo is the insertion point: the first block whose first_key > seek key.
        // If the seek key is before all blocks, lo == 0; if after all, lo == len.

        earliest_match.unwrap_or_else(|| lo.min(self.block_meta.len() - 1))
    }

    /// Direct point lookup for a single key. Returns `Some(raw_value)` if found,
    /// `None` if not. Uses BlockIterator for the within-block search — much
    /// lighter than creating a full SsTableIterator.
    ///
    /// Returns `Bytes` directly from the block cache via zero-copy slice.
    /// The returned `Bytes` shares the cached block's underlying buffer —
    /// no heap allocation on the read path.
    #[allow(dead_code)]
    pub(crate) fn point_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.point_get_with_hash(key, bloom::hash_key(key))
    }

    /// Like `point_get`, but accepts a precomputed bloom hash to avoid
    /// recomputing it when the same key is probed across multiple L0 SSTs.
    pub(crate) fn point_get_with_hash(&self, key: &[u8], bloom_hash: u32) -> Result<Option<Bytes>> {
        self.point_get_with_hash_inner(key, bloom_hash, None)
            .map(|opt| opt.map(|(val, _found_key)| val))
    }

    /// Like `point_get_with_hash`, but also returns the full encoded key of
    /// the found entry.  Used by the MVCC lookup path to compare timestamps
    /// across multiple SSTs.
    ///
    /// When `read_ts` is provided (MVCC mode), returns the newest version
    /// whose commit timestamp is <= `read_ts`.  Scans forward through
    /// versions of the same user key if the first match is too new.
    pub(crate) fn point_get_with_hash_and_key(
        &self,
        key: &[u8],
        bloom_hash: u32,
        read_ts: Option<u64>,
    ) -> Result<Option<(Bytes, Vec<u8>)>> {
        self.point_get_with_hash_inner(key, bloom_hash, read_ts)
    }

    fn point_get_with_hash_inner(
        &self,
        key: &[u8],
        bloom_hash: u32,
        read_ts: Option<u64>,
    ) -> Result<Option<(Bytes, Vec<u8>)>> {
        if !self.should_probe_point_key(key, bloom_hash) {
            return Ok(None);
        }

        let (blk_idx, blk_iter) = self.seek_point_get_block_iter(key)?;
        if TS_ENABLED {
            return self.point_get_mvcc_match(key, read_ts, blk_idx, blk_iter);
        }

        if let Some(found) = Self::point_get_non_mvcc_match(key, &blk_iter) {
            return Ok(Some(found));
        }
        Ok(None)
    }

    fn should_probe_point_key(&self, key: &[u8], bloom_hash: u32) -> bool {
        // Range-only SSTs have no point keys to probe.
        if self.first_key.is_none() || self.last_key.is_none() {
            return false;
        }

        // Key range check — compare encoded keys (byte-order preserved).
        // With MVCC, the search key uses ts=u64::MAX (smallest encoded form for the
        // user key), so it may sort before the SST's first_key. We skip the range
        // check in MVCC mode and rely on exact matching after seek.
        if !TS_ENABLED
            && let (Some(first), Some(last)) = (self.first_key.as_ref(), self.last_key.as_ref())
            && (key < first.raw_ref() || key > last.raw_ref())
        {
            return false;
        }

        // Legacy SSTs (v2-v4) used the old persisted bloom hash and must
        // bypass bloom pruning after reopen. New SSTs (v5+) use the stable
        // cross-process hash and can safely prune here.
        if self.stable_bloom_hash
            && let Some(ref bloom) = self.bloom
            && !bloom.may_contain(bloom_hash)
        {
            return false;
        }

        !self.block_meta.is_empty()
    }

    fn hinted_block_idx_for_key(&self, key: &[u8]) -> Option<usize> {
        // Compare user-key prefixes only (stripping the MVCC timestamp suffix)
        // so the hint works correctly in MVCC mode where the search key
        // carries u64::MAX but block metadata keys carry real timestamps.
        let hinted = self
            .last_block_hint
            .load(std::sync::atomic::Ordering::Relaxed);
        if hinted >= self.block_meta.len() {
            return None;
        }
        let meta = &self.block_meta[hinted];
        let fk = meta.first_key.encoded_user_key();
        let lk = meta.last_key.encoded_user_key();
        let user_key = crate::key::encoded_user_key_prefix(key).unwrap_or(key);
        if user_key < fk || user_key > lk {
            return None;
        }

        Some(hinted)
    }

    fn initial_point_get_block_idx(&self, key: &[u8]) -> usize {
        match self.hinted_block_idx_for_key(key) {
            Some(hinted) => hinted,
            None => {
                let idx = self.find_block_idx(KeySlice::from_slice(key));
                self.last_block_hint
                    .store(idx, std::sync::atomic::Ordering::Relaxed);
                idx
            }
        }
    }

    fn seek_point_get_block_iter(
        &self,
        key: &[u8],
    ) -> Result<(usize, crate::block::BlockIterator)> {
        let mut blk_idx = self.initial_point_get_block_idx(key);
        let mut blk_iter = self.read_block_iter_for_key(blk_idx, key)?;

        // If the block iterator is positioned before the target key (can happen
        // when encoded keys shift block boundaries), advance to subsequent blocks.
        while !blk_iter.is_valid() || blk_iter.key().raw_ref() < key {
            blk_idx += 1;
            if blk_idx >= self.num_of_blocks() {
                break;
            }
            blk_iter = self.read_block_iter_for_key(blk_idx, key)?;
        }

        Ok((blk_idx, blk_iter))
    }

    fn read_block_iter_for_key(
        &self,
        blk_idx: usize,
        key: &[u8],
    ) -> Result<crate::block::BlockIterator> {
        let block = self.read_block_cached(blk_idx)?;
        Ok(crate::block::BlockIterator::create_and_seek_to_key(
            block,
            KeySlice::from_slice(key),
        ))
    }

    fn point_get_mvcc_match(
        &self,
        key: &[u8],
        read_ts: Option<u64>,
        mut blk_idx: usize,
        mut blk_iter: crate::block::BlockIterator,
    ) -> Result<Option<(Bytes, Vec<u8>)>> {
        let seek_uk = crate::key::encoded_user_key_prefix(key).unwrap_or(key);

        loop {
            while blk_iter.is_valid() {
                let found_key = blk_iter.key();
                let found_uk = found_key.encoded_user_key();
                if found_uk != seek_uk {
                    break;
                }
                let ts = found_key.ts();
                if read_ts.is_none_or(|rts| ts <= rts) {
                    return Ok(Some((blk_iter.value_bytes(), found_key.raw_ref().to_vec())));
                }
                blk_iter.next();
            }

            if blk_iter.is_valid() {
                break;
            }

            blk_idx += 1;
            if blk_idx >= self.num_of_blocks() {
                break;
            }
            blk_iter = self.read_block_iter_for_key(blk_idx, key)?;
        }

        Ok(None)
    }

    fn point_get_non_mvcc_match(
        key: &[u8],
        blk_iter: &crate::block::BlockIterator,
    ) -> Option<(Bytes, Vec<u8>)> {
        if blk_iter.is_valid() && blk_iter.key().raw_ref() == key {
            return Some((blk_iter.value_bytes(), blk_iter.key().raw_ref().to_vec()));
        }

        None
    }

    /// Get number of data blocks.
    #[must_use]
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    #[must_use]
    pub fn first_key(&self) -> Option<&KeyBytes> {
        self.first_key.as_ref()
    }

    #[must_use]
    pub fn last_key(&self) -> Option<&KeyBytes> {
        self.last_key.as_ref()
    }

    /// Returns the first point key, panicking if this is a range-only SST.
    ///
    /// Use this in compaction/iterator code that cannot handle range-only SSTs.
    #[must_use]
    pub fn first_key_or_panic(&self) -> &KeyBytes {
        self.first_key
            .as_ref()
            .expect("range-only SST has no point keys")
    }

    /// Returns the last point key, panicking if this is a range-only SST.
    #[must_use]
    pub fn last_key_or_panic(&self) -> &KeyBytes {
        self.last_key
            .as_ref()
            .expect("range-only SST has no point keys")
    }

    /// Returns `true` if this SST contains range-tombstone fragments.
    #[must_use]
    pub fn has_range_tombstones(&self) -> bool {
        self.range_tombstones.is_some()
    }

    /// Returns the cached range-tombstone fragments, if any.
    #[must_use]
    pub fn range_tombstone_fragments(
        &self,
    ) -> Option<&Arc<[crate::range_tombstone::RangeTombstoneFragment]>> {
        self.range_tombstones.as_ref()
    }

    /// Returns `true` if this is a range-only SST (no point data, only range tombstones).
    #[must_use]
    pub fn is_range_only(&self) -> bool {
        self.first_key.is_none() && self.range_tombstones.is_some()
    }

    /// Returns the tombstone range `[min_start, max_end)` for this SST,
    /// computed from the cached range-tombstone fragments.
    /// Returns `None` if the SST has no range tombstones.
    ///
    /// Fragments must be sorted by `start` (invariant from `fragment_range`).
    #[must_use]
    pub fn tombstone_range(&self) -> Option<(&[u8], &[u8])> {
        let frags = self.range_tombstones.as_ref()?;
        if frags.is_empty() {
            return None;
        }
        debug_assert!(
            frags.windows(2).all(|w| w[0].start <= w[1].start),
            "fragments must be sorted by start"
        );
        Some((frags[0].start.as_ref(), frags[frags.len() - 1].end.as_ref()))
    }

    /// Get table size in bytes
    #[must_use]
    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    /// Snapshot candidate-scoring stats for this SST.
    ///
    /// This is intentionally non-hot-path and may touch SST data blocks.
    pub fn mvcc_gc_stats(self: Arc<Self>) -> Result<SsTableMvccGcStats> {
        if let Some(cached) = self.mvcc_gc_stats_cache.get() {
            return Ok(*cached);
        }

        let mut point_tombstone_count = 0u64;
        let mut redundant_version_count = 0u64;
        if !self.is_range_only() {
            let mut iter =
                SsTableIterator::create_and_seek_to_first(self.clone(), CacheAdmission::Bypass)?;
            let mut prev_user_key: Option<Vec<u8>> = None;

            while iter.is_valid() {
                let key = iter.key();
                let user_key = key.encoded_user_key();
                if prev_user_key
                    .as_ref()
                    .is_some_and(|prev| prev.as_slice() == user_key)
                {
                    redundant_version_count += 1;
                } else {
                    prev_user_key = Some(user_key.to_vec());
                }

                if crate::vlog::KvKind::is_tombstone_value(iter.raw_value()) {
                    point_tombstone_count += 1;
                }

                iter.next()?;
            }
        }

        let range_tombstone_fragment_count = self
            .range_tombstone_fragments()
            .map_or(0, |frags| frags.len() as u64);
        let range_tombstone_metadata_bytes = self.range_tombstone_fragments().map_or(0, |frags| {
            frags
                .iter()
                .map(|f| {
                    8 + f.start.len() as u64 + f.end.len() as u64 + f.covering_ts.len() as u64 * 8
                })
                .sum()
        });

        let stats = SsTableMvccGcStats {
            sst_id: self.sst_id(),
            level: None,
            max_ts: self.max_ts(),
            min_ttl_expire_ts: self.ttl_metadata.min_ttl_expire_ts,
            max_ttl_expire_ts: self.ttl_metadata.max_ttl_expire_ts,
            has_non_ttl_entries: self.ttl_metadata.has_non_ttl_entries,
            ttl_entry_count: self.ttl_metadata.ttl_entry_count,
            total_entry_count: self.ttl_metadata.total_entry_count,
            point_tombstone_count,
            range_tombstone_fragment_count,
            range_tombstone_metadata_bytes,
            redundant_version_count,
        };
        // Cache the result. set() returns Err if already set; this is expected
        // under concurrent access and we simply use the cached value.
        let _ = self.mvcc_gc_stats_cache.set(stats);

        Ok(stats)
    }

    pub fn range_overlap(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        // Range-only SSTs always overlap (they cover a range with tombstones).
        let (Some(first), Some(last)) = (self.first_key.as_ref(), self.last_key.as_ref()) else {
            return true;
        };
        let lo = first.as_key_slice();
        let hi = last.as_key_slice();

        // Both TS_ENABLED and non-TS paths compare encoded keys directly
        // since the encoding preserves byte order.
        match lower {
            Bound::Included(x) if KeySlice::from_slice(x) > hi => return false,
            Bound::Excluded(x) if KeySlice::from_slice(x) >= hi => return false,
            _ => {}
        };

        match upper {
            Bound::Included(y) if KeySlice::from_slice(y) < lo => return false,
            Bound::Excluded(y) if KeySlice::from_slice(y) <= lo => return false,
            _ => {}
        };

        true
    }

    /// Check if this SST may contain keys with the given prefix.
    ///
    /// Returns `true` when no prefix bloom metadata is available (old SST or
    /// prefix bloom disabled). Returns `false` only when the prefix bloom
    /// filter proves the SST cannot contain matching keys.
    pub(crate) fn may_contain_prefix(&self, prefix: &[u8]) -> bool {
        if !self.stable_bloom_hash {
            return true;
        }
        let Some(ref prefix_blooms) = self.prefix_blooms else {
            return true;
        };
        let Some(filter) = prefix_blooms.best_filter_for(prefix.len()) else {
            return true;
        };
        debug_assert!(prefix.len() >= filter.prefix_len);
        filter
            .bloom
            .may_contain(bloom::hash_key(&prefix[..filter.prefix_len]))
    }
}
