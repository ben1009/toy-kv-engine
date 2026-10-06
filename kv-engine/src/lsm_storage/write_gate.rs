//! Protect the active memtable through synchronous guards or owned async leases.
//!
//! A lease registers under a short read guard. Every exclusive operation then
//! blocks new registration and drains existing leases before touching the
//! memtable. Async commits never hold a blocking lock while awaiting WAL I/O or
//! MVCC publication. Freeze, checkpoint and GC CAS use the same exclusive gate.

use std::sync::Arc;

use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct ActiveMemtableGate {
    lock: RwLock<()>,
    leases: Arc<ReadLeases>,
    available: Notify,
}

#[derive(Default)]
struct ReadLeases {
    count: Mutex<usize>,
    drained: Condvar,
}

pub(crate) struct MemtableLease {
    leases: Arc<ReadLeases>,
}

pub(crate) struct ActiveMemtableWriteGuard<'a> {
    guard: Option<RwLockWriteGuard<'a, ()>>,
    gate: &'a ActiveMemtableGate,
}

impl ActiveMemtableGate {
    pub(crate) fn read(&self) -> RwLockReadGuard<'_, ()> {
        self.lock.read()
    }

    pub(crate) async fn read_async(&self) -> MemtableLease {
        let notification = self.available.notified();
        tokio::pin!(notification);
        loop {
            notification.as_mut().enable();
            if let Some(_guard) = self.lock.try_read() {
                let mut count = self.leases.count.lock();
                *count = count.checked_add(1).expect("memtable lease count overflow");
                return MemtableLease {
                    leases: Arc::clone(&self.leases),
                };
            }
            notification.as_mut().await;
            notification.set(self.available.notified());
        }
    }

    pub(crate) fn write(&self) -> ActiveMemtableWriteGuard<'_> {
        let guard = self.lock.write();
        let mut count = self.leases.count.lock();
        while *count != 0 {
            self.leases.drained.wait(&mut count);
        }

        ActiveMemtableWriteGuard {
            guard: Some(guard),
            gate: self,
        }
    }

    #[cfg(test)]
    pub(crate) fn exclusive_pending_for_test(&self) -> bool {
        self.lock.try_read().is_none()
    }

    #[cfg(test)]
    pub(crate) fn try_write(&self) -> Option<ActiveMemtableWriteGuard<'_>> {
        let guard = self.lock.try_write()?;
        let guard = ActiveMemtableWriteGuard {
            guard: Some(guard),
            gate: self,
        };
        if *self.leases.count.lock() != 0 {
            return None;
        }

        Some(guard)
    }
}

impl Drop for MemtableLease {
    fn drop(&mut self) {
        let mut count = self.leases.count.lock();
        *count = count
            .checked_sub(1)
            .expect("live lease owns its registration");
        if *count == 0 {
            self.leases.drained.notify_all();
        }
    }
}

impl Drop for ActiveMemtableWriteGuard<'_> {
    fn drop(&mut self) {
        // Unlock before waking readers so they can register immediately.
        self.guard.take();
        self.gate.available.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    #[tokio::test(flavor = "current_thread")]
    async fn exclusive_operation_drains_leases_without_blocking_the_executor() {
        let gate = Arc::new(ActiveMemtableGate::default());
        let lease = gate.read_async().await;
        assert!(gate.try_write().is_none());
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::task::spawn_blocking(move || {
            let _guard = writer_gate.write();
        });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished());
        drop(lease);
        writer.await.unwrap();
        assert!(gate.try_write().is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_reader_wakes_after_exclusive_operation_unlocks() {
        let gate = ActiveMemtableGate::default();
        let guard = gate.write();
        let reader = gate.read_async();
        tokio::pin!(reader);
        assert!(
            reader
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        drop(guard);
        let lease = reader.await;
        drop(lease);
        assert!(gate.try_write().is_some());
    }
}
