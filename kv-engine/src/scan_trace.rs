use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScanTraceSnapshot {
    pub block_loads: u64,
    pub sst_switches: u64,
}

thread_local! {
    static TRACE: Cell<ScanTraceSnapshot> = const { Cell::new(ScanTraceSnapshot {
        block_loads: 0,
        sst_switches: 0,
    }) };
}

pub(crate) fn reset() {
    TRACE.set(ScanTraceSnapshot::default());
}

pub(crate) fn snapshot() -> ScanTraceSnapshot {
    TRACE.get()
}

pub(crate) fn note_block_load() {
    TRACE.with(|cell| {
        cell.set_with(|state| ScanTraceSnapshot {
            block_loads: state.block_loads + 1,
            ..state
        });
    });
}

pub(crate) fn note_sst_switch() {
    TRACE.with(|cell| {
        cell.set_with(|state| ScanTraceSnapshot {
            sst_switches: state.sst_switches + 1,
            ..state
        });
    });
}

trait CellExt<T: Copy> {
    fn set_with(&self, f: impl FnOnce(T) -> T);
}

impl<T: Copy> CellExt<T> for Cell<T> {
    fn set_with(&self, f: impl FnOnce(T) -> T) {
        self.set(f(self.get()));
    }
}
