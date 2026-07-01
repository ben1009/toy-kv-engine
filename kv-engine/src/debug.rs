use crate::lsm_storage::{KvEngine, LsmStorageInner};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        print!("{}", self.dump_structure_string());
    }

    pub fn dump_structure_string(&self) -> String {
        let snapshot = self.state.load();
        let mut out = String::new();
        if !snapshot.l0_sstables.is_empty() {
            out.push_str(&format!(
                "L0 ({}): {:?}\n",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            ));
        }
        for (level, files) in &snapshot.levels {
            out.push_str(&format!("L{level} ({}): {:?}\n", files.len(), files));
        }
        out
    }
}

impl KvEngine {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }

    pub fn dump_structure_string(&self) -> String {
        self.inner.dump_structure_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm_storage::LsmStorageOptions;

    #[test]
    fn dump_structure_string_fresh_engine_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let opts = LsmStorageOptions::default_for_test();
        let engine = KvEngine::open(&db_path, opts).expect("open");
        let s = engine.dump_structure_string();
        // Fresh engine may have empty levels — just verify it doesn't panic.
        assert!(
            s.lines()
                .all(|line| line.starts_with('L') || line.is_empty())
        );
        engine.close().expect("close");
    }

    #[test]
    fn dump_structure_string_after_write() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let opts = LsmStorageOptions::default_for_test();
        let engine = KvEngine::open(&db_path, opts).expect("open");
        engine.put(b"k", b"v").expect("put");
        engine.force_flush().expect("flush");
        let s = engine.dump_structure_string();
        // After flush, there should be at least one SST in L0
        assert!(!s.is_empty(), "engine should have SSTs after flush: {s:?}");
        assert!(s.contains("L0"), "should show L0 level: {s:?}");
        engine.close().expect("close");
    }
}
