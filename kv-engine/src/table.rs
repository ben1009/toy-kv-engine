#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::{fs::File, mem, ops::Bound, path::Path, sync::Arc};

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use self::bloom::Bloom;
use crate::{
    block::{Block, SIZE_OF_U16},
    key::{Key, KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

const SIZE_OF_U32: usize = mem::size_of::<u32>();

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
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
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
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bloom_offset = file
            .read(file.size() - SIZE_OF_U32 as u64, SIZE_OF_U32 as u64)?
            .as_slice()
            .get_u32() as u64;
        let bloom = bloom::Bloom::decode(
            file.read(
                bloom_offset,
                file.size() - bloom_offset - SIZE_OF_U32 as u64,
            )?
            .as_slice(),
        )?;

        let meta_offset = file
            .read(bloom_offset - SIZE_OF_U32 as u64, SIZE_OF_U32 as u64)?
            .as_slice()
            .get_u32() as u64;
        let block_meta = BlockMeta::decode_block_meta(
            file.read(meta_offset, bloom_offset - SIZE_OF_U32 as u64 - meta_offset)?
                .as_slice(),
        );
        let first_key = block_meta[0].first_key.clone();
        let last_key = block_meta[block_meta.len() - 1].last_key.clone();
        Ok(Self {
            file,
            block_meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        })
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
        let ret = Block::decode(data.as_slice());

        Ok(Arc::new(ret))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let ret = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;

            return Ok(ret);
        }

        self.read_block(block_idx)
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut lo = 0;
        let mut hi = self.block_meta.len() - 1;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            if key >= self.block_meta[mid].first_key.as_key_slice()
                && key <= self.block_meta[mid].last_key.as_key_slice()
            {
                return mid;
            }

            if key < self.block_meta[mid].first_key.as_key_slice() {
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

        lo
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    /// Get table size in bytes
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

        match lower {
            Bound::Included(x) => {
                let x = KeySlice::from_slice(x);
                if x > hi {
                    return false;
                }
            }
            Bound::Excluded(x) => {
                let x = KeySlice::from_slice(x);
                if x >= hi {
                    return false;
                }
            }
            _ => {}
        };

        match upper {
            Bound::Included(y) => {
                let y = KeySlice::from_slice(y);
                if y < lo {
                    return false;
                }
            }
            Bound::Excluded(y) => {
                let y = KeySlice::from_slice(y);
                if y <= lo {
                    return false;
                }
            }
            _ => {}
        };

        true
    }
}
