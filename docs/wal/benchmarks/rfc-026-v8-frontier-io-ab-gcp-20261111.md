# RFC 026 V8 FRONTIER I/O A/B on GCP — 2026-11-11

## Result

Changing only the V8 Parallel FRONTIER write handle from buffered I/O to
`O_DIRECT` consistently reduced the time spent in `fdatasync`. It did not
improve throughput: the direct arm issued more durability advances, offsetting
the lower latency per sync. This identifies buffered FRONTIER I/O as a strong
contributor to the slow syscall distribution, but not as a standalone
throughput fix.

The experiment uses a benchmark-only mode switch. It does not change the
default WAL behavior. Run crash-recovery qualification before considering a
runtime change.

## Setup

- GCP `c4-standard-16-lssd`, 16 writers, two Local SSDs in RAID0, ext4.
- V8 Parallel, 100,000 point puts per run, 1 KiB values, WAL enabled, no
  compaction.
- Each ABBA block runs buffered, direct, direct, buffered FRONTIER writes.
- Two ABBA blocks ran without syscall profiling (four samples per mode). One
  additional ABBA block collected `fdatasync` syscall histograms with bpftrace
  (two samples per mode).
- Every run reported DATA `O_DIRECT=true`, a stable active WAL during the run,
  and DATA/FRONTIER/sync descriptors on the same device and inode. The sync
  descriptor remained buffered in both arms.

## Results

Unprofiled medians across two ABBA blocks:

| Metric | Buffered FRONTIER | Direct FRONTIER | Change |
| --- | ---: | ---: | ---: |
| Throughput | 20.22k puts/s | 19.85k puts/s | -1.8% |
| `fdatasync` calls | 7,490 | 9,312 | +24.3% |
| Mean `fdatasync` time | 169.7 μs | 106.3 μs | -37.4% |
| Sampled operation p99 | 3.08 ms | 2.32 ms | -24.6% |

The p99 values are noisy and are included for context; this small run set does
not qualify a tail-latency improvement.

Kernel syscall histograms from the bpftrace ABBA block:

| Syscall percentile | Buffered FRONTIER | Direct FRONTIER |
| --- | --- | --- |
| p50 | 128–256 μs (both samples) | 64–128 μs (both samples) |
| p95 | 128–256 μs (both samples) | 64–128 μs (both samples) |
| p99 | 128–256 μs; 256–512 μs | 128–256 μs (both samples) |

The traced syscall counts were within 0.3% of the WAL pipeline's sync counts.
With bpftrace enabled, median throughput was 19.98k puts/s buffered and
19.91k puts/s direct; median sync counts were 7,524 and 8,988 respectively.

## Interpretation

The kernel traces confirm that the syscall-latency shift is real, rather than
only an artifact of user-space timing. Keeping the inode, DATA mode, sync
descriptor, workload, and durability protocol constant makes FRONTIER write
mode the relevant changed variable. The result supports buffered FRONTIER
writeback as a major source of the high V8 `fdatasync` latency.

The direct-arm mean of about 106 μs remains well above the earlier V7 result of
about 15 μs. Those values come from separate runs, so the residual gap needs a
matched comparison; this experiment does not explain the full V7/V8 difference.

Direct FRONTIER writes also changed group timing: they produced about 24% more
syncs in the unprofiled runs. Throughput stayed essentially flat across the
ABBA blocks, so the saved time per sync was consumed by more frequent
durability advances. The reason for the increased sync count remains open.

## Follow-up

Investigate why direct FRONTIER completion increases durability-advance
frequency and whether coalescing can preserve the lower syscall latency without
increasing sync count. Keep the experiment mode behind the `bench` feature
until crash-recovery tests cover direct FRONTIER writes and the existing
DATA-completion → FRONTIER → sync → durable-publication order.

The GCP VM, boot disk, firewall, subnet, and isolated network were destroyed
after artifact collection. Raw JSON and bpftrace outputs remain in the local
`/tmp/gcp-v8-frontier-ab-20261111` experiment directory; they are not committed
to the repository.
