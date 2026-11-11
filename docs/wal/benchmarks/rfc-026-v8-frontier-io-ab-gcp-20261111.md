# RFC 026 V8 FRONTIER I/O A/B on GCP — 2026-11-11

## Result

Changing only the V8 Parallel FRONTIER write handle from buffered I/O to
`O_DIRECT` consistently reduced the time spent in `fdatasync`. It did not
improve throughput: the direct arm issued more durability advances, offsetting
the lower latency per sync. This identifies buffered FRONTIER I/O as a strong
contributor to the slow syscall distribution, but not as a standalone
throughput fix. Follow-up stage timings also found that O_DIRECT FRONTIER
writes increased accumulated FRONTIER-write-plus-sync time by 19.4% per run;
the lower syscall latency does not make this a throughput optimization.

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

## Combined FRONTIER write and `fdatasync` timing follow-up

The earlier A/B timed `fdatasync` but not the FRONTIER write itself. This
follow-up used the per-advance observations added in commit
`98bd2bb3853f32239f04588dc4641cef3eb83dc8`, without broad profiling. Two
unprofiled ABBA blocks ran in the order buffered, direct, direct, buffered;
each run contained 100,000 Point puts with 16 writers and 1 KiB values.

| Metric | Buffered FRONTIER | Direct FRONTIER | Change |
| --- | ---: | ---: | ---: |
| Throughput, median of four runs | 20.05k puts/s | 19.65k puts/s | -2.0% |
| Sampled operation p50, median | 0.782 ms | 0.779 ms | — |
| Sync advances per run, median | 7,478 | 9,251 | +23.7% |
| Mean tickets per advance | 13.39 | 10.78 | -19.5% |
| FRONTIER write p50 / p95 per advance | 4.4 / 32.4 μs | 40.9 / 262.0 μs | |
| `fdatasync` p50 / p95 per advance | 167.8 / 193.1 μs | 104.7 / 127.0 μs | |
| FRONTIER write + `fdatasync` p50 / p95 per advance | 173.5 / 212.3 μs | 148.6 / 374.4 μs | |
| Sum of per-advance write + sync timings, median per run | 1.360 s | 1.624 s | +19.4% |

Direct FRONTIER reduced median `fdatasync` time by 37.6%, but the O_DIRECT
FRONTIER write itself took longer: its median rose from 4.4 μs to 40.9 μs and
its p95 from 32.4 μs to 262.0 μs. The combined per-advance median improved,
while its p95 worsened. More frequent, smaller advances raised the sum of
FRONTIER-write-plus-sync timings by 19.4% per run, and throughput fell by 2.0%.
Thus, the faster syscall does not translate into a faster durability path or
better throughput in this workload.

Sampled operation p99 medians were 4.40 ms buffered and 3.13 ms direct, but
the run ranges overlapped (3.77–4.50 ms and 2.15–4.26 ms respectively); this
small sample does not establish a stable tail-latency improvement.

All eight runs reported DATA `O_DIRECT=true`, FRONTIER mode matching the
selected arm, and a buffered sync descriptor. The three handles referred to
the same device and inode; start/end metadata remained stable, and every
recorded sync observation succeeded. Sync counts matched observation counts.
This was a performance experiment only; it did not run crash-recovery
qualification.

## Follow-up

Investigate the higher direct-write cost and smaller groups, then test whether
controlled coalescing reduces the accumulated durability cost. Keep the
experiment mode behind the `bench` feature until crash-recovery tests cover
direct FRONTIER writes and the existing DATA-completion → FRONTIER → sync →
durable-publication order.

The GCP VM, boot disk, firewall, subnet, and isolated network were destroyed
after artifact collection. Raw JSON and bpftrace outputs remain in the local
`/tmp/gcp-v8-frontier-ab-20261111` experiment directory. The per-advance JSON
for this follow-up is in local `/tmp/gcp-v8-durability-diagnostics-20261111`;
neither set of raw artifacts is committed to the repository.
