use std::{ops::Deref, path::Path, sync::Arc};

use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions};
use tempfile::TempDir;

/// Owns a benchmark engine until its background work has stopped, then removes
/// its temporary data. Return it from a timed routine to defer cleanup until
/// Criterion has stopped the timer.
pub struct TempEngine {
    engine: Arc<KvEngine>,
    dir: TempDir,
}

impl TempEngine {
    pub fn new(options: LsmStorageOptions) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("toy-kv-bench-")
            .tempdir()
            .expect("create benchmark directory");

        Self::open(dir, options)
    }

    pub fn open(dir: TempDir, options: LsmStorageOptions) -> Self {
        let engine = KvEngine::open(dir.path(), options).expect("open benchmark engine");

        Self { engine, dir }
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }
}

impl Deref for TempEngine {
    type Target = Arc<KvEngine>;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

impl Drop for TempEngine {
    fn drop(&mut self) {
        // KvEngine::Drop detaches its workers. Explicit close joins them before
        // TempDir is removed, including when benchmark code unwinds.
        if let Err(error) = self.engine.close() {
            eprintln!("benchmark engine cleanup failed: {error:#}");
        }
    }
}
