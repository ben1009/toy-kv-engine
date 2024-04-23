#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

// TODO: gc the wals when the related imm_memtable got flushed
impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let f = File::create_new(path.as_ref()).context("failed to create WAL")?;

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(f))),
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

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(f))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = vec![];

        buf.put_u16(key.len() as u16);
        buf.put(key);

        buf.put_u16(value.len() as u16);
        buf.put(value);

        file.write_all(&buf).context("failed to write to WAL")
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;

        file.get_ref()
            .sync_all()
            .context("failed to sync WAL to disk")
    }
}
