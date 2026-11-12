# RFC 026 V8 bounded extent preparation on GCP — 2026-11-12

## Conclusion

Benchmark-only sliding `fallocate` increased V8 Point throughput by 58–59% over
the baseline without extra extent preparation in this 16-writer run. The 1 MiB and 4 MiB windows had
nearly identical throughput; this experiment does not establish that either
window is better. It supports bounded extent preparation as a strong
optimization candidate, but does not qualify a production change.

The measured matrix did not collect latency percentiles or kernel tracepoints.
It therefore cannot confirm whether sliding preparation reduced ext4
`-EAGAIN` returns or io-wq fallback in these particular runs. The earlier
[extent-preparation diagnosis](rfc-026-v8-ext4-extent-preparation-gcp-20261112.md)
contains the separate trace evidence for one-shot preparation.

## Experiment

The host was a 16-logical-CPU C4 with eight physical cores, two Local SSD NVMe
devices, an ext4 filesystem on `/dev/md127`, and kernel `7.0.0-1017-gcp`.
Each measured run performed 100,000 1 KiB Point puts with 16 writers. V8 used
Parallel I/O, Direct FRONTIER, and a fixed 400 μs coalescing budget. The three
arms were:

| Arm | Extent preparation | Window |
| --- | --- | ---: |
| Baseline | None | — |
| Sliding 1 MiB | `fallocate` before submitting writes that reach an unprepared range | 1 MiB |
| Sliding 4 MiB | `fallocate` before submitting writes that reach an unprepared range | 4 MiB |

There were six measured runs per arm, in three counterbalanced cycles of
`baseline, 1 MiB, 4 MiB, 4 MiB, 1 MiB, baseline`. Profiling and per-sync
diagnostics were disabled. Aggregate WAL counters remained enabled. The
sliding `fallocate` operations ran as the workload advanced and are included
in the measured write duration; there was no one-time pre-timed allocation or
zero initialization.

| Arm | Throughput, median (range) | Process CPU, median | Syncs, median | Mean `fdatasync`, median | FRONTIER write + sync, median cumulative | Sliding preparation per run |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Baseline | 21.32k/s (21.02–21.35k) | 11.34 s | 7,366 | 104.2 μs | 1.088 s | — |
| Sliding 1 MiB | 33.76k/s (33.68–34.67k) | 7.10 s | 6,332.5 | 99.2 μs | 0.894 s | 416 calls; 416 MiB; 17.3 ms accumulated |
| Sliding 4 MiB | 33.93k/s (33.43–34.59k) | 7.09 s | 6,334 | 98.9 μs | 0.892 s | 104 calls; 416 MiB; 4.6 ms accumulated |

Against baseline, the median throughput gain was 58.4% for the 1 MiB window and
59.2% for the 4 MiB window. Every sliding-preparation run exceeded every
baseline run. The 4 MiB median was only 0.5% above the 1 MiB median, with
overlapping run ranges, so there is no demonstrated throughput advantage for
the larger window.

Both sliding arms used about 14% fewer syncs, about 5% lower mean `fdatasync`
time, about 37% less process CPU time, and about 18% less cumulative FRONTIER
write-plus-sync time than baseline. These aggregate changes accompany the
throughput increase; this run does not isolate their causal contributions.

The benchmark JSON confirmed stable I/O metadata from start to finish for all
runs. DATA and FRONTIER used `O_DIRECT`; the sync handle was buffered. All three
handles referred to the same WAL inode. No p50/p99 latency samples were
collected, so this result makes no tail-latency claim.

## Interpretation

Sliding `fallocate` covers the active WAL range in bounded increments before
the corresponding DATA or FRONTIER write is submitted. It avoids the large
one-time zero-initialization pass and recorded only 4.6–17.3 ms of accumulated
allocation time per run. The large, repeatable throughput difference is
consistent with extent preparation removing costly work from the V8 write
path, but this matrix did not trace ext4 fallback or io-wq events and cannot
prove that mechanism by itself.

The controls remain gated by the `bench` feature; no production default or WAL
protocol changed. The code's recovery qualification matrix includes the
sliding mode across both FRONTIER modes and the existing crash points. A wider
qualification should cover writer counts and batch workloads, and should
collect latency percentiles before considering production adoption.

## Provenance and cleanup

- Tested source: `27dd978c118a1c26aa1da07bff2bbc5fee89a1f0`.
- Release `write-perf` binary SHA-256:
  `1c3dba1fa03c824e4d2e391b894a675f08e9577c93063311804a3db80a10fcb9`.
- Raw per-run JSON remains in the local temporary results directory and is
  not included in the PR. No tar archive was uploaded.
- Terraform destroyed the five temporary resources. Independent project-scoped
  checks confirmed that the VM, auto-delete boot disk, SSH firewall rule,
  subnet, and isolated VPC were absent after teardown.
- CI for the tested source commit passed before this documentation update.

No production default is changed by these results.
