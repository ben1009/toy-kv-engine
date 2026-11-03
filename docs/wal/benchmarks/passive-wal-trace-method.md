# Passive WAL trace regimes

[capture_wal_trace.py](capture_wal_trace.py) records the real benchmark's NVMe
and block events. [wal_trace_regimes.py](wal_trace_regimes.py) reconstructs
command latencies, detects supported regimes, and joins application metrics by
`CLOCK_MONOTONIC` time. There are no extra detection writes or flushes.

This is a diagnostic method. A label describes an observed latency regime for a
specific request shape; it does not establish the SSD's internal firmware state,
identical completion patterns, or RFC performance qualification. Keep the reversed
V7/V8 controls. A shared label alone does not eliminate workload or order effects.
The earlier [probe-based protocol](paired-v7-v8-method.md) and its historical
rejections remain unchanged.

## Capture

The benchmark's opt-in `bench` feature hook records every completed batch's start,
end, and put count. A separate sampler brackets process CPU and cumulative WAL
counter reads, normally every second. Both use `CLOCK_MONOTONIC`, matching perf's
explicit `--clockid mono`. Per-writer request vectors and sampled counters stay
in memory. The benchmark serializes its timeline after close; the capture runner
points that output to an inherited memfd. All perf streams, benchmark JSON,
timelines and stderr remain in RAM until the whole cohort stops.

```bash
cargo build --release --package kv-engine --features bench --bin write-perf
sudo python3 -B docs/wal/benchmarks/capture_wal_trace.py \
  --binary target/release/write-perf \
  --output-dir target/passive-wal-trace --cpus 0-15
python3 -B docs/wal/benchmarks/wal_trace_regimes.py \
  --capture-dir target/passive-wal-trace
```

Choose a CPU mask available on the host. Root is used for system-wide perf access;
the child clears supplementary groups, drops to the invoking UID/GID, and then
executes the benchmark. `--smoke` performs just two 1,024-put point captures.
A normal capture predeclares V7 → V8 → V8 → V7 for each of:

| Workload | Puts | Writers | Batch size | Value bytes |
|---|---:|---:|---:|---:|
| Point via batch API | 20,000 | 1 | 1 | 1,024 |
| Batch64 | 750,000 | 16 | 64 | 1,024 |

Both use Parallel, PITR, no compaction, and a 2 GiB SST target. Setup/maintenance
and close lie outside the application timeline's window. A capture has a 180-second
process timeout; only its own process group is terminated on timeout. The runner
uses new disposable database paths, never raw-device writes or controller changes.
Plan, hashes, boot ID, event formats, parameters and policy are saved before capture.
It refuses binary or boot changes. Do not run builds/tests alongside measurements.

The required tracepoints are:

```text
nvme:nvme_setup_cmd
nvme:nvme_complete_rq
block:block_rq_issue
block:block_rq_complete
```

Their layouts are read from the running kernel's tracefs. `perf` is sufficient;
`trace-cmd` and `bpftrace` are alternatives, not required packages. Linux documents
[tracepoint discovery and perf capture](https://docs.kernel.org/trace/tracepoint-analysis.html).

## Signal and rejection rules

The current detector is calibrated only for this host's `nvme0n1`, namespace 1,
512-byte logical blocks and **isolated ordinary 4 KiB writes**. The parser is for
native little-endian `PERFILE2` files with explicit monotonic clocks. Command
identity is controller + queue + command ID; an ID may be reused after completion.
Nonpositive durations, live-ID collisions, retries, errors, missing endpoints or
lost records invalidate classification for the capture. All four event types
must be observed. Unmatched capture-boundary events also fail closed.

For each successful FLUSH completion, inspect the **first subsequent namespace
WRITE submission**, not the first write to complete. That write must belong to
the benchmark process, have 4 KiB length and zero NVMe control bits, and overlap
no namespace I/O. There must also be no in-flight I/O at the flush completion.
A later overlapping command invalidates an initially isolated write. A write
cannot serve multiple flush samples. An unsuitable first write is recorded with
reasons; a later convenient 4 KiB write is never substituted.

NVMe PID attribution identifies an issuing process, not a file or DATA-versus-
FRONTIER record. Filesystem journal traffic and kernel-issued writes are not
silently attributed to WAL DATA. Nonzero control includes FUA, defined as bit 14
in the [kernel NVMe definitions](https://raw.githubusercontent.com/torvalds/linux/v6.12/include/linux/nvme.h).
The block events remain context: [block completion can be partial](https://raw.githubusercontent.com/torvalds/linux/v6.12/include/trace/events/block.h),
and these trace payloads contain no stable request pointer. Pairing them just by
sector would be unsafe.

The predeclared regime policy is:

| Setting | Value |
|---|---|
| Rolling median | 16 eligible samples |
| Confirmation | 5 consecutive qualifying windows |
| Fast | median below 100 µs |
| Slow | median above 400 µs |
| Middle band | unknown |
| Maximum sample gap | 250 ms |
| Boundary guard | 20 ms on each side |

Confirmation starts at the confirming sample, never backdated. A changing median
withdraws uncertain history back to the rolling window's earliest supporting
write. Ineligible observations and excessive gaps reset evidence. Unknown gaps
are not forward-filled. These thresholds are distinct from the old probe gate;
they are not used to relabel its archived data. Larger writes and overlapping
parallel traffic need separate calibration before they can produce labels.

## Output and joins

The analyzer emits per-run raw sample/analysis JSON, `storage-regimes.csv`, and
`interval-metrics.csv`. Times are absolute monotonic nanoseconds, with run,
format and workload columns. Gaps between capture windows have no inferred state.
Each metrics row preserves its actual duration, completed puts, throughput, raw-
request p50/p99, process CPU delta, sync count, worker notification requests and
actual eventfd writes. `reported_commit_groups` retains its format-dependent
meaning: V7 counts DATA groups; V8 counts FRONTIER publications.

An interval is classified only when its entire counter-acquisition envelope
fits inside one guarded regime and all requests completing in it started inside
that regime. A crossing request rejects the **whole interval**, preventing
selective removal of slow requests. Intervals shorter than 250 ms are retained
as unknown; p99 requires at least 100 completed requests. Percentiles are computed
from those request durations, never averaged across windows. Operation and CPU/
WAL-counter conservation are checked. The cohort exporter deliberately produces
no V8/V7 performance ratio.

The sampler and perf have observer overhead. No untraced overhead control or
controlled-completion replay has been established here. If there are no supported
intervals for a format, report that absence rather than relax the signal or choose
favorable windows after seeing throughput. A future protocol can predeclare a
calibrated bulk-I/O signal or finer metric windows, then make fresh captures.
