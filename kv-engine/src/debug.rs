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
