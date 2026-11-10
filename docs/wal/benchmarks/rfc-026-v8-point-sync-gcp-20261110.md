# GCP WAL Point-path diagnostic — 2026-11-10

## Scope

This continues the investigation of commit `40ce6f65`; its profiling binary source was `54c1a7392f50b2809b123d7359b78e050bd2303d`.

The initial data in the supplied report pointed to fixed per-commit cost: V8 point throughput lagged V7 by about 50–57%, while Batch16/64 had caught up. On the profiled 16-writer point case, V7/V8 had 8,408/8,086 syncs, 15.4/174.6 μs average sync time, and 6.37/12.35 s process CPU. The unprofiled 16-writer medians were 48.6k/20.4k puts/s for V7/V8 point, 215.1k/147.0k for Batch8, 262.5k/275.1k for Batch16, and 589.2k/610.4k for Batch64. The suggested priorities were to validate sync latency with a same-host ABBA run, then separate V8 pipeline stages and single-writer Leader/Parallel costs; BLAKE3 and ticket-queue rewrites were lower priority.

## Setup

The benchmark binary was built from the exact source commit above with pinned Rust nightly `nightly-2026-09-23`; SHA-256: `26e843c91a96162695f35fd20a884d0704465c1db79df0185597f718fbaa23d4`.

The temporary GCP host was `c4-standard-16-lssd` in `us-east1-b`: 16 vCPUs, Intel Xeon 6985P-C / Granite Rapids, two 375 GB NVMe Local SSDs in RAID0, ext4 (`noatime,stripe=256`). The benchmark used `wal_concurrent`, 1 KiB values, 100,000 puts, 2 GiB target SST, no compaction, PITR V7/V8, and parallel I/O unless stated otherwise. Throughput runs had no `--profile` and no eBPF attachment. Separate eBPF and detailed-observation runs are diagnostics, not scored runs.

The original target was `us-central1-c`; GCP could not allocate this shape in `us-central1-a`, `-b`, `-c`, or `-f`. The same machine shape was available in `us-east1-b`, so comparisons are controlled within this run, but the zone differs from the earlier report. Terraform used a one-hour lifetime and a $2 estimated-spend guard; billing was not changed. After archiving results, Terraform destroyed all five managed resources (instance, subnet, firewall, VPC, and lifetime guard). An independent `gcloud` query found no remaining instance, network, or firewall.

## Unprofiled 16-writer Point ABBA

Eight runs, order `V7, V8, V8, V7, V8, V7, V7, V8`:

| Format | Throughput median | Throughput runs (k puts/s) | p50 latency median | p99 latency median | Sync count median | Runtime `fdatasync` mean |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| V7 | 49.25k puts/s | 42.37, 49.28, 49.22, 49.33 | 0.298 ms | 0.773 ms | 8,197 | 15.3 μs |
| V8 | 20.62k puts/s | 20.59, 20.60, 20.68, 20.63 | 0.771 ms | 1.385 ms | 7,292 | 166.4 μs |

V8 throughput was 58.1% below V7. Its sync count was about 11% lower, while measured time per sync was 10.9× higher. Median process CPU time was 11.25 s for V8 versus 5.86 s for V7; elapsed times were about 4.85 s and 2.03 s respectively. This does not look like a shortage of the 16 vCPUs.

## Direct syscall trace

A separate counterbalanced V7/V8/V8/V7 cohort used bpftrace on `sys_enter_fdatasync`/`sys_exit_fdatasync`, filtered to the benchmark PID:

| Format | Per-run count | Per-run mean syscall time | Distribution |
| --- | --- | --- | --- |
| V7 | 8,167 / 8,140 | 17 / 14 μs | Median about 1 μs; roughly 71–73% under 2 μs, with a visible 64–128 μs tail |
| V8 | 7,425 / 7,331 | 172 / 166 μs | About 96–99% in the 128–256 μs bucket |

The kernel syscall trace independently confirms the runtime counters. The anomaly is in the V8 sync path under this workload, not just in sampling proportions or profiler overhead.

## Single-writer V8 Leader versus Parallel

Unprofiled ABBA order `Leader, Parallel, Parallel, Leader`, 100,000 puts each:

| Mode | Median throughput | `fdatasync` mean | p99 latency median | Cumulative sync-wait counter |
| --- | ---: | ---: | ---: | ---: |
| Leader | 4.05k puts/s | 177.6 μs | 0.526 ms | 6 ms |
| Parallel | 3.31k puts/s | 168.3 μs | 0.598 ms | 29.4 s |

Leader was 22.6% faster. Both modes had 100,000 syncs and similar per-call sync latency. Parallel's `wal_sync_wait_ns` was much larger; this counter includes durability waiting, so it is a phase clue rather than a clean measure of coordination overhead. Both single-writer runs reported zero worker eventfd writes. This is consistent with two costs: a V8 sync cost shared by both modes, plus a Parallel wait-path cost.

## Separate detailed per-sync diagnostic

With `--profile` enabled only for this diagnostic cohort (not for throughput scoring), V7's per-sync observation median was about 1 μs, P95 107–109 μs, and P99 124–139 μs. V8's median was 165 μs, P95 186–187 μs, and P99 about 200 μs. V8 observations usually saw the next ticket's frames ready and digest folded, while its data was not yet complete; mean groups covered per sync was about 13.8. This suggests data-write completion and sync sequencing deserve investigation, but does not prove which dirty pages or file state caused the kernel latency.

## Next optimization point

First investigate the V8 durability path shared by Leader and Parallel: why its actual `fdatasync` syscalls take roughly 10–11× longer than V7 on the same C4 host and ext4 RAID0 volume. Then measure the Parallel producer wait separately from time inside `fdatasync`.

The next diagnostic should correlate per-sync syscall latency with DATA-write completion, FRONTIER-write timing, file descriptor/inode, dirty bytes, and ticket readiness. Only after that attribution should we choose between changing write ordering/coalescing and adding a small-transaction fast path. These measurements do not justify changing BLAKE3 or rewriting the ticket queue.

## Raw artifacts

The raw run archive is retained locally and is not included in this PR. It contains per-run logs, command lines, and host metadata; the report includes the aggregate measurements and setup details used for this analysis. No repository source files were modified.
