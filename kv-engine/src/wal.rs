// REMOVE THIS LINE after fully implementing this functionality
use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    os::unix::io::AsRawFd,
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::io_uring::UringWriter;

pub struct Wal {
    inner: Arc<Mutex<(BufWriter<File>, UringWriter)>>,
}

// WAL files are garbage-collected by LsmStorageInner::force_flush_next_imm_memtable
// once the corresponding immutable memtable has been durably flushed to SST.
impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let f = File::create_new(path.as_ref()).context("failed to create WAL")?;
        let uring = UringWriter::new(4).context("failed to create io_uring for WAL")?;

        Ok(Self {
            inner: Arc::new(Mutex::new((BufWriter::new(f), uring))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut f = File::options()
            .read(true)
            .append(true)
            .open(path.as_ref())
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        let mut data = &buf[..];
        while data.has_remaining() {
            let key_size = data.get_u16() as usize;
            let key = &data[..key_size];
            data.advance(key_size);

            let value_size = data.get_u16() as usize;
            let value = &data[..value_size];
            data.advance(value_size);

            skiplist.insert(Bytes::from(key.to_owned()), Bytes::from(value.to_owned()));
        }

        let uring = UringWriter::new(4).context("failed to create io_uring for WAL")?;

        Ok(Self {
            inner: Arc::new(Mutex::new((BufWriter::new(f), uring))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        anyhow::ensure!(
            key.len() <= u16::MAX as usize,
            "WAL key too large: {}",
            key.len()
        );
        anyhow::ensure!(
            value.len() <= u16::MAX as usize,
            "WAL value too large: {}",
            value.len()
        );

        let mut inner = self.inner.lock();
        let file = &mut inner.0;
        let mut buf = vec![];

        buf.put_u16(key.len() as u16);
        buf.put(key);

        buf.put_u16(value.len() as u16);
        buf.put(value);

        file.write_all(&buf).context("failed to write to WAL")
    }

    /// Implement this in MVCC.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        let (ref mut file, ref mut uring) = *inner;
        file.flush()?;
        uring
            .fsync(file.get_ref().as_raw_fd())
            .context("failed to sync WAL to disk")
    }
}
