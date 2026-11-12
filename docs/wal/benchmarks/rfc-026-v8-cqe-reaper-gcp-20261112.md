# RFC 026 V8 CQE-to-reaper timeline on GCP — 2026-11-12

## Purpose

This diagnostic separates kernel I/O completion from userspace completion handling and contiguous-prefix publication. It follows the V8 DATA path from `io_uring_submit_req` through kernel CQE, userspace observation, group result delivery, reaper receipt, ready-prefix advancement, and durable publication.

The run used the current PR branch's benchmark-only `--wal-completion-timeline` instrumentation, sampling one request per 128 request IDs. A bpftrace probe recorded the sampled request submissions and every CQE from the corresponding WAL io_uring context, allowing each sampled group to be joined to its kernel completion events.

## Setup and limits

The temporary GCP host had 16 vCPUs, two Local SSDs in RAID0, and ext4. Both arms used V8 Parallel, 16 writers, 100,000 point puts, 1 KiB values, Direct FRONTIER, and a fixed 400 μs coalescing budget. The WAL helper was not pinned. The arms differed only in the io-wq CPU mask: all available CPUs, then CPU 8.

Each arm was run once, in that order. The kernel probe emitted 100,000 CQE records and 782 sampled submit records per arm; the application timeline reported 782 samples and zero dropped samples. No per-sync or HOL diagnostics were enabled. The bpftrace probe observed every completion and is intrusive, so the throughput and CPU numbers below are context for these traces, not qualified performance scores. The runtime's disabled HOL counter fields must not be interpreted as zero measured HOL.

Terraform destroyed all five temporary resources after collection. An independent GCP query found no matching VM, network, subnet, or firewall remaining.

## Stage timings

Values are p50 / p95 / p99 in microseconds over the 782 sampled request groups.

| Interval | io-wq all CPUs | io-wq CPU 8 |
| --- | ---: | ---: |
| Kernel `io_uring_submit_req` → kernel `io_uring_complete` | 259.5 / 360.6 / 455.2 | 73.4 / 232.3 / 287.6 |
| Kernel CQE → userspace observes sampled CQE | 5.2 / 21.9 / 30.8 | 8.6 / 27.4 / 35.0 |
| Last group CQE → userspace marks group finished | 5.9 / 23.6 / 30.8 | 9.8 / 29.0 / 35.8 |
| Group finished → result send starts | 1.5 / 5.1 / 8.3 | 1.5 / 4.7 / 8.3 |
| Result send starts → reaper receives result | 10.5 / 20.1 / 30.0 | 10.0 / 19.6 / 37.8 |
| Reaper receives result → contiguous prefix is ready | 1.6 / 221.5 / 262.0 | 1.1 / 88.4 / 133.0 |
| Prefix ready → durable publication | 306.1 / 601.6 / 1,047.2 | 402.8 / 643.7 / 843.8 |

The last-group-CQE interval uses the latest kernel completion among the request IDs in that sampled group. It therefore estimates the time from the group's final kernel CQE to userspace group closure, rather than only the sampled request's own CQE.

## Observations

- Most sampled latency occurs before the kernel emits a CQE. With the CPU 8 mask, kernel submit-to-CQE p50 was 73.4 μs versus 259.5 μs with all CPUs, while p95 was 232.3 μs versus 360.6 μs. This interval includes io-wq scheduling and execution, filesystem work, and storage service; it does not isolate NVMe device latency.
- Kernel CQE observation and group closure are comparatively short. Their p95 values were below 30 μs in both arms. The result channel and reaper handoff also remained short, with p95 at 20.1 μs and 19.6 μs.
- Reaper receipt to contiguous-prefix readiness has a substantial tail: p95 was 221.5 μs with all CPUs and 88.4 μs with CPU 8. This is consistent with prefix waiting or coordinator scheduling, but the trace does not identify the specific predecessor ticket responsible.
- Prefix-ready to durable publication remains hundreds of microseconds. This interval includes the configured coalescing wait, FRONTIER work, sync, and publication; it is not a pure sync measurement.
- The traced runs reported 20.33k ops/s and 23.42k ops/s, with 7,575 and 6,337 syncs respectively. These single, sequential, heavily traced runs do not establish a repeatable throughput gain.

## Conclusion and next experiment

This trace does not show a long userspace CQE-to-reaper bottleneck. The larger sampled delay is before the kernel CQE, and the CPU 8 run also had a shorter reaper-to-prefix tail. The evidence therefore points to both kernel-side request completion latency and contiguous-prefix waiting as relevant intervals; it does not yet establish whether the latter is caused by out-of-order I/O completion, coordinator scheduling, or both.

The next diagnostic should record the blocking predecessor ticket when a completed group cannot advance the prefix, then correlate that ticket's submit and kernel-CQE times. A repeated counterbalanced placement run should follow with a multi-CPU io-wq sweep; CPU 8 remains a diagnostic placement, not a proposed production limit. Do not infer that reducing io-wq parallelism is the desired optimization.

No raw trace or archive is included in the PR; this report retains the aggregate measurements and their limits.
