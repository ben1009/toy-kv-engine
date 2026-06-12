pub(crate) mod bloom;
mod builder;
mod iterator;

use std::{fs::File, mem, ops::Bound, path::Path, sync::Arc};

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use self::bloom::Bloom;
use crate::{
    block::{Block, SIZE_OF_U16},
    key::{Key, KeyBytes, KeySlice, TS_ENABLED},
    lsm_storage::BlockCache,
};

const SIZE_OF_U32: usize = mem::size_of::<u32>();
const SIZE_OF_U64: usize = mem::size_of::<u64>();

/// Magic number for MVCC-format SSTs: "MVCC" in ASCII.
const SST_MVCC_MAGIC: u32 = 0x4D56_4343;
/// Footer version for MVCC-format SSTs (no prefix bloom).
const SST_FOOTER_VERSION_V2: u8 = 2;
/// Footer version for SSTs with prefix bloom filters.
const SST_FOOTER_VERSION_V3: u8 = 3;
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
            estimated_size += std::mem::size_of::<u16>();

            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.first_key.len();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.last_key.len();
        }
        // offsets length
        estimated_size += std::mem::size_of::<u16>();

        estimated_size
    }

    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut offsets = vec![];
        buf.reserve(BlockMeta::estimated_size(block_meta));

        for meta in block_meta {
            // Note: offsets are stored as u16 in the existing format — truncation
            // is intentional and matches the decode side. This limits individual
            // block meta entries to 65535 byte positions within the metadata section.
            offsets.push(buf.len() as u16);

            buf.put_u16(meta.first_key.len() as u16);
            buf.put(meta.first_key.raw_ref());

            buf.put_u16(meta.last_key.len() as u16);
            buf.put(meta.last_key.raw_ref());

            buf.put_u16(meta.offset as u16);
        }

        for o in &offsets {
            buf.put_u16(*o);
        }
        buf.put_u16(offsets.len() as u16);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let data = buf.chunk();
        let num_of_elements = (&data[data.len() - SIZE_OF_U16..]).get_u16() as usize;
        let offset = data.len() - SIZE_OF_U16 - num_of_elements * SIZE_OF_U16;
        let mut datas = &data[0..offset];

        let mut ret = vec![];
        for _ in 0..num_of_elements {
            let first_key_len = datas.get_u16() as usize;
            let first_key = &datas[..first_key_len];
            datas.advance(first_key_len);

            let last_key_len = datas.get_u16() as usize;
            let last_key = &datas[..last_key_len];
            datas.advance(last_key_len);

            let offset = datas.get_u16() as usize;

            ret.push(BlockMeta {
                offset,
                first_key: Key::from_vec(first_key.to_vec()).into_key_bytes(),
                last_key: Key::from_vec(last_key.to_vec()).into_key_bytes(),
            })
        }

        ret
    }
}

/// A file object.
#[derive(Debug)]
pub struct FileObject(Option<File>, u64);

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
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in MVCC.
    max_ts: u64,
    /// Prefix bloom filters for prefix scan pruning. Present only in v3 SSTs.
    pub(crate) prefix_blooms: Option<PrefixBloomSet>,
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
        // Read the tail region in a single I/O call.  v3 footer needs 21 bytes
        // (bloom_offset:4 + prefix_bloom_offset:4 + max_ts:8 + magic:4 + version:1),
        // v2 needs 17 bytes (bloom_offset:4 + max_ts:8 + magic:4 + version:1).
        // Read 21 to cover both cases.
        let v3_tail_size = MVCC_FOOTER_EXTRA as usize + SIZE_OF_U32 + SIZE_OF_U32;
        let tail_start = file.size().saturating_sub(v3_tail_size as u64);
        let tail = file.read(tail_start, file.size() - tail_start)?;

        // Detect MVCC footer by checking magic AND version at their expected
        // positions. Version can be 2 (no prefix bloom) or 3 (with prefix bloom).
        let version = tail[tail.len() - 1];
        let has_mvcc_magic = tail.len() >= (MVCC_FOOTER_EXTRA as usize + SIZE_OF_U32)
            && (&tail[tail.len() - 5..tail.len() - 1]).get_u32() == SST_MVCC_MAGIC;
        if has_mvcc_magic && version != SST_FOOTER_VERSION_V2 && version != SST_FOOTER_VERSION_V3 {
            anyhow::bail!(
                "unsupported MVCC footer version: {} (expected {} or {})",
                version,
                SST_FOOTER_VERSION_V2,
                SST_FOOTER_VERSION_V3
            );
        }
        let is_mvcc = has_mvcc_magic
            && (version == SST_FOOTER_VERSION_V2 || version == SST_FOOTER_VERSION_V3);

        let (bloom_offset_base, max_ts, bloom_offset, prefix_bloom_set) = if is_mvcc
            && version == SST_FOOTER_VERSION_V3
        {
            // v3 file layout (from end of file):
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
            let bloom_off = file
                .read(prefix_bloom_off - SIZE_OF_U32 as u64, SIZE_OF_U32 as u64)?
                .as_slice()
                .get_u32() as u64;
            // The prefix bloom section ends where the prefix_bloom_offset
            // field is written (footer_start - 4).
            let section_end = footer_start - SIZE_OF_U32 as u64;

            // Decode prefix bloom section.
            let prefix_blooms = Self::decode_prefix_blooms(&file, prefix_bloom_off, section_end)?;

            // bloom_offset_base = start of prefix bloom section (bloom data
            // ends right before it).
            (prefix_bloom_off, max_ts, bloom_off, prefix_blooms)
        } else if is_mvcc {
            // v2 tail layout: [bloom_offset: u32][max_ts: u64][magic: u32][version: u8]
            //                 bytes 0..4    bytes 4..12   bytes 12..16  byte 16
            let footer = &tail[tail.len() - MVCC_FOOTER_EXTRA as usize..];
            let max_ts = (&footer[0..8]).get_u64();
            let footer_start = file.size() - MVCC_FOOTER_EXTRA;
            let bloom_off = (&tail[tail.len() - MVCC_FOOTER_EXTRA as usize - SIZE_OF_U32
                ..tail.len() - MVCC_FOOTER_EXTRA as usize])
                .get_u32() as u64;
            (footer_start, max_ts, bloom_off, None)
        } else {
            // Legacy footer: last 4 bytes are bloom_offset.
            let bloom_off = (&tail[tail.len() - SIZE_OF_U32..]).get_u32() as u64;
            (file.size(), 0, bloom_off, None)
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
            meta_offset
                .checked_add(SIZE_OF_U16 as u64)
                .is_some_and(|sum| sum <= bloom_offset - SIZE_OF_U32 as u64),
            "SST meta block too small or out of bounds: meta_offset={}, bloom_offset={}",
            meta_offset,
            bloom_offset
        );
        let block_meta = BlockMeta::decode_block_meta(
            file.read(meta_offset, bloom_offset - SIZE_OF_U32 as u64 - meta_offset)?
                .as_slice(),
        );
        anyhow::ensure!(!block_meta.is_empty(), "SST has no block metadata");
        let first_key = block_meta[0].first_key.clone();
        let last_key = block_meta[block_meta.len() - 1].last_key.clone();
        // Reject footer-less MVCC SSTs: scanning only first_key/last_key per
        // block cannot recover the true max_ts (a middle key may carry a higher
        // timestamp). Underestimating max_ts would allow commit timestamp reuse
        // after restart, violating MVCC safety. Require the v2 footer.
        // Keys with ts=0 are the default for non-MVCC usage and are not a concern.
        if max_ts == 0 {
            // Validate both the key structure (decode_user_key) and timestamp
            // (extract_ts) to reduce false positives on legacy binary keys
            // that coincidentally contain a timestamp-shaped suffix.
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
        Ok(Self {
            file,
            block_meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts,
            prefix_blooms: prefix_bloom_set,
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
        let section = file.read(prefix_bloom_offset, section_size)?;
        let filter_count = (&section[0..2]).get_u16() as usize;
        anyhow::ensure!(
            filter_count > 0,
            "SST v3 prefix bloom section has filter_count=0"
        );
        let table_len = 2 + filter_count * (2 + 4 + 4); // u16 + u32 + u32 per entry
        anyhow::ensure!(
            section_size as usize >= table_len,
            "SST prefix bloom section too small for {} filters: need {} bytes, have {}",
            filter_count,
            table_len,
            section_size
        );
        let filter_bytes_start = table_len;
        let filter_bytes_len = section_size as usize - filter_bytes_start;
        let mut filters = Vec::with_capacity(filter_count);
        let mut prev_prefix_len = 0usize;
        let mut prev_end = 0u32;
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
                    filter_offset as u32 >= prev_end,
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
            prev_end = filter_end as u32;
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
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
            prefix_blooms: None,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let lo = self.block_meta[block_idx].offset as u64;
        let hi = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset) as u64;

        let data = self.file.read(lo, hi - lo)?;
        let ret = Block::decode_from_vec(data)?;

        Ok(Arc::new(ret))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            return block_cache.try_get_with(self.id, block_idx, || self.read_block(block_idx));
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
        // Key range check — compare encoded keys (byte-order preserved).
        // With MVCC, the search key uses ts=u64::MAX (smallest encoded form for the
        // user key), so it may sort before the SST's first_key. We skip the range
        // check in MVCC mode and rely on the bloom filter for fast rejection.
        if !TS_ENABLED && (key < self.first_key.raw_ref() || key > self.last_key.raw_ref()) {
            return Ok(None);
        }
        // Bloom filter check.  The caller precomputes hash_key(raw_user_key),
        // which equals hash_key(decode(encode(user_key, MAX))) — the same hash
        // the bloom filter was built with.  Use it directly for both MVCC and
        // non-MVCC paths, avoiding a redundant decode + rehash per SST probe.
        if let Some(ref bloom) = self.bloom
            && !bloom.may_contain(bloom_hash)
        {
            return Ok(None);
        }
        // Defensive guard: meta-only SSTs have empty block_meta
        if self.block_meta.is_empty() {
            return Ok(None);
        }
        // Find candidate block via metadata binary search.
        // With suffix-timestamp format, `find_block_idx` already searches for
        // the earliest matching block via `earliest_match`. No extra backward
        // search is needed here.
        let mut blk_idx = self.find_block_idx(KeySlice::from_slice(key));
        let mut block = self.read_block_cached(blk_idx)?;
        let mut blk_iter =
            crate::block::BlockIterator::create_and_seek_to_key(block, KeySlice::from_slice(key));
        // If the block iterator is positioned before the target key (can happen
        // when encoded keys shift block boundaries), advance to subsequent blocks.
        while !blk_iter.is_valid() || blk_iter.key().raw_ref() < key {
            blk_idx += 1;
            if blk_idx >= self.num_of_blocks() {
                break;
            }
            block = self.read_block_cached(blk_idx)?;
            blk_iter = crate::block::BlockIterator::create_and_seek_to_key(
                block,
                KeySlice::from_slice(key),
            );
        }
        if TS_ENABLED {
            let seek_uk = crate::key::encoded_user_key_prefix(key).unwrap_or(key);
            // Scan forward through versions of the same user key (newest
            // first due to inverted timestamp ordering) to find the newest
            // version visible at `read_ts`.  May span multiple blocks.
            loop {
                while blk_iter.is_valid() {
                    let found_key = blk_iter.key();
                    let found_uk = found_key.encoded_user_key();
                    if found_uk != seek_uk {
                        // Different user key — no more versions in this block.
                        // Check if a later block might have more versions (can
                        // happen when encoded keys shift block boundaries).
                        break;
                    }
                    let ts = found_key.ts();
                    if read_ts.is_none_or(|rts| ts <= rts) {
                        return Ok(Some((blk_iter.value_bytes(), found_key.raw_ref().to_vec())));
                    }
                    // This version is too new — advance to the next version
                    blk_iter.next();
                }
                // The current block had only too-new versions or a different
                // user key.  Try the next block in case the user key spans a
                // block boundary.
                if !blk_iter.is_valid() {
                    blk_idx += 1;
                    if blk_idx >= self.num_of_blocks() {
                        break;
                    }
                    block = self.read_block_cached(blk_idx)?;
                    blk_iter = crate::block::BlockIterator::create_and_seek_to_key(
                        block,
                        KeySlice::from_slice(key),
                    );
                    // Continue the outer loop to scan this new block
                    continue;
                }
                // Different user key — no point checking later blocks
                break;
            }
        } else if blk_iter.is_valid() && blk_iter.key().raw_ref() == key {
            return Ok(Some((
                blk_iter.value_bytes(),
                blk_iter.key().raw_ref().to_vec(),
            )));
        }
        Ok(None)
    }

    /// Get number of data blocks.
    #[must_use]
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    #[must_use]
    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    #[must_use]
    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
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

    pub fn range_overlap(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        let lo = self.first_key.as_key_slice();
        let hi = self.last_key.as_key_slice();

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
