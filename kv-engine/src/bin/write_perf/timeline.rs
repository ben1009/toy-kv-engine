//! Opt-in benchmark observations. No files are written during timed writes.

use std::{
    fs::File,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::Serialize;

use super::kv_engine_wrapper::lsm_storage::KvEngine;

#[derive(Debug, Serialize)]
pub(super) struct RequestSpan {
    pub start_ns: u64,
    pub end_ns: u64,
    pub puts: u64,
}

#[derive(Serialize)]
struct CounterPoint {
    sample_begin_ns: u64,
    sample_end_ns: u64,
    user_cpu_ms: f64,
    system_cpu_ms: f64,
    sync_count: u64,
    reported_commit_groups: u64,
    worker_notification_requests: u64,
    worker_eventfd_writes: u64,
}

impl CounterPoint {
    fn capture(engine: &KvEngine) -> Result<Self> {
        let sample_begin_ns = monotonic_ns()?;
        let cpu = super::process_cpu_times().context("process CPU clock unavailable")?;
        let profile = engine.write_profile();

        Ok(Self {
            sample_begin_ns,
            sample_end_ns: monotonic_ns()?,
            user_cpu_ms: cpu.user_ms,
            system_cpu_ms: cpu.system_ms,
            sync_count: profile.wal_sync_count,
            reported_commit_groups: profile.wal_commit_groups,
            worker_notification_requests: profile.wal_worker_eventfd_notifications,
            worker_eventfd_writes: profile.wal_worker_eventfd_writes,
        })
    }
}

pub(super) struct Capture {
    destination: PathBuf,
    interval_ms: u64,
    engine: Arc<KvEngine>,
    initial: Option<CounterPoint>,
    stop: Arc<AtomicBool>,
    sampler: Option<JoinHandle<Result<Vec<CounterPoint>>>>,
    extent_helper: Option<PathBuf>,
    database: PathBuf,
    initial_extents: Option<serde_json::Value>,
}

pub(super) struct Output {
    destination: PathBuf,
    interval_ms: u64,
    points: Vec<CounterPoint>,
    requests: Vec<RequestSpan>,
    wal_extents: Option<serde_json::Value>,
}

impl Capture {
    pub(super) fn from_env(engine: Arc<KvEngine>, database: &Path) -> Result<Option<Self>> {
        let Some(destination) = std::env::var_os("TOYKV_WAL_TIMELINE") else {
            return Ok(None);
        };
        let interval_ms = interval_ms(
            std::env::var("TOYKV_WAL_TIMELINE_INTERVAL_MS")
                .ok()
                .as_deref(),
        )?;
        let extent_helper = std::env::var_os("TOYKV_WAL_EXTENT_HELPER").map(PathBuf::from);
        let initial_extents = extent_helper
            .as_ref()
            .map(|helper| extent_snapshot(helper, database, false))
            .transpose()?;
        let initial = CounterPoint::capture(&engine)?;
        let stop = Arc::new(AtomicBool::new(false));
        let sampler_stop = stop.clone();
        let sampler_engine = engine.clone();
        let sampler = std::thread::Builder::new()
            .name("wal-bench-timeline".into())
            .spawn(move || {
                let mut points = Vec::new();
                loop {
                    std::thread::park_timeout(Duration::from_millis(interval_ms));
                    if sampler_stop.load(Ordering::Acquire) {
                        break;
                    }
                    points.push(CounterPoint::capture(&sampler_engine)?);
                }
                Ok(points)
            })?;

        Ok(Some(Self {
            destination: destination.into(),
            interval_ms,
            engine,
            initial: Some(initial),
            stop,
            sampler: Some(sampler),
            extent_helper,
            database: database.to_owned(),
            initial_extents,
        }))
    }

    fn stop_sampler(&mut self) -> Result<Vec<CounterPoint>> {
        self.stop.store(true, Ordering::Release);
        let Some(sampler) = self.sampler.take() else {
            return Ok(Vec::new());
        };
        sampler.thread().unpark();

        sampler
            .join()
            .map_err(|_| anyhow::anyhow!("timeline sampler panicked"))?
    }

    pub(super) fn finish(mut self, mut requests: Vec<RequestSpan>) -> Result<Output> {
        let mut points = vec![
            self.initial
                .take()
                .context("timeline initial point missing")?,
        ];
        points.extend(self.stop_sampler()?);
        points.push(CounterPoint::capture(&self.engine)?);
        // Only query extents and read frame bytes after the final timed counter.
        // The helper writes JSON to a pipe; the capture runner persists it later.
        let wal_extents = self
            .extent_helper
            .as_ref()
            .map(|helper| {
                let after = extent_snapshot(helper, &self.database, true)?;
                Ok::<_, anyhow::Error>(serde_json::json!({
                    "before": self.initial_extents.take(),
                    "after": after,
                }))
            })
            .transpose()?;
        requests.sort_unstable_by_key(|span| span.end_ns);
        ensure!(
            requests
                .iter()
                .all(|span| span.start_ns < span.end_ns && span.puts > 0),
            "invalid benchmark request span"
        );
        Ok(Output {
            destination: std::mem::take(&mut self.destination),
            interval_ms: self.interval_ms,
            points,
            requests,
            wal_extents,
        })
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        let _ = self.stop_sampler();
    }
}

impl Output {
    pub(super) fn persist(self) -> Result<()> {
        serde_json::to_writer(
            File::create(self.destination)?,
            &serde_json::json!({
                "schema": "kv-engine.wal-benchmark-timeline.v1",
                "clock": "CLOCK_MONOTONIC",
                "process_id": std::process::id(),
                "interval_ms": self.interval_ms,
                "counter_points": self.points,
                "requests": self.requests,
                "counter_acquisition_is_bracketed": true,
                "wal_extents": self.wal_extents,
                "latency_samples": "all completed application batches; no percentile averaging",
            }),
        )?;

        Ok(())
    }
}

fn extent_snapshot(helper: &Path, database: &Path, inventory: bool) -> Result<serde_json::Value> {
    let mut command = Command::new("python3");
    command
        .arg("-B")
        .arg(helper)
        .arg("--snapshot")
        .arg(database);
    if inventory {
        command.arg("--inventory");
    }
    let output = command
        .output()
        .context("run read-only WAL extent helper")?;
    ensure!(
        output.status.success(),
        "WAL extent helper failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).context("decode WAL extent snapshot")
}

fn interval_ms(value: Option<&str>) -> Result<u64> {
    let value = value.map(str::parse).transpose()?.unwrap_or(1000);
    ensure!(
        (100..=5000).contains(&value),
        "timeline interval must be 100..=5000 ms"
    );

    Ok(value)
}

pub(super) fn monotonic_ns() -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
        // SAFETY: clock_gettime initializes this valid output pointer on success.
        let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, value.as_mut_ptr()) };
        ensure!(
            result == 0,
            "CLOCK_MONOTONIC failed: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: the successful clock_gettime initialized both fields.
        let value = unsafe { value.assume_init() };

        Ok(value.tv_sec as u64 * 1_000_000_000 + value.tv_nsec as u64)
    }
    #[cfg(not(target_os = "linux"))]
    anyhow::bail!("WAL timeline capture requires Linux CLOCK_MONOTONIC")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_interval_is_bounded_and_explicit() {
        assert_eq!(interval_ms(None).unwrap(), 1000);
        assert_eq!(interval_ms(Some("100")).unwrap(), 100);
        assert_eq!(interval_ms(Some("5000")).unwrap(), 5000);
        for value in ["0", "99", "5001", "-1", "invalid"] {
            assert!(interval_ms(Some(value)).is_err());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn timeline_clock_is_nondecreasing() {
        let before = monotonic_ns().unwrap();
        let after = monotonic_ns().unwrap();
        assert!(after >= before);
    }
}
