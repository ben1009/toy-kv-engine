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

Individual unprofiled runs are listed below. Latency percentiles use one-in-100 operation sampling; process CPU is user plus system time. Runtime `fdatasync` mean is the per-run pipeline counter divided by its sync count.

| Run | Format | Throughput (k puts/s) | p50 (ms) | p99 (ms) | Syncs | Runtime `fdatasync` mean (μs) | Process CPU (s) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 01 | V7 | 42.37 | 0.335 | 1.082 | 8,168 | 19.9 | 5.83 |
| 02 | V8 | 20.59 | 0.774 | 1.330 | 7,304 | 166.6 | 11.31 |
| 03 | V8 | 20.60 | 0.766 | 1.441 | 7,298 | 166.1 | 11.29 |
| 04 | V7 | 49.28 | 0.300 | 0.746 | 8,193 | 14.8 | 5.85 |
| 05 | V8 | 20.68 | 0.770 | 1.040 | 7,251 | 165.5 | 11.22 |
| 06 | V7 | 49.22 | 0.295 | 0.790 | 8,260 | 15.1 | 5.93 |
| 07 | V7 | 49.33 | 0.296 | 0.756 | 8,200 | 15.6 | 5.87 |
| 08 | V8 | 20.63 | 0.771 | 1.486 | 7,285 | 167.0 | 11.20 |

V8 throughput was 58.1% below V7. Its sync count was about 11% lower, while measured time per sync was 10.9× higher. Median process CPU time was 11.25 s for V8 versus 5.86 s for V7; elapsed times were about 4.85 s and 2.03 s respectively. This does not look like a shortage of the 16 vCPUs.

## Direct syscall trace

A separate counterbalanced V7/V8/V8/V7 cohort used bpftrace on `sys_enter_fdatasync`/`sys_exit_fdatasync`, filtered to the benchmark PID. The table groups the bpftrace log2 buckets into ranges; counts and percentages are per run.

| Trace order | Format | Syscalls | Mean (μs) | <2 μs | 2–64 μs | 64–128 μs | 128–256 μs | ≥256 μs |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 01 | V7 | 8,167 | 17 | 5,919 (72.5%) | 1,140 (14.0%) | 967 (11.8%) | 70 (0.9%) | 71 (0.9%) |
| 02 | V8 | 7,425 | 172 | 0 (0%) | 0 (0%) | 43 (0.6%) | 7,151 (96.3%) | 231 (3.1%) |
| 03 | V8 | 7,331 | 166 | 0 (0%) | 0 (0%) | 53 (0.7%) | 7,218 (98.5%) | 60 (0.8%) |
| 04 | V7 | 8,140 | 14 | 5,811 (71.4%) | 1,236 (15.2%) | 1,031 (12.7%) | 62 (0.8%) | 0 (0%) |

The kernel syscall trace independently confirms the runtime counters. The anomaly is in the V8 sync path under this workload, not just in sampling proportions or profiler overhead.

## Source-level FRONTIER I/O hypothesis

Source inspection at commit `a2801cff` shows a concrete I/O-mode difference. V7 Parallel opens the WAL with `O_DIRECT` and clones that handle for its worker and sync paths ([V7 setup](https://github.com/ben1009/toy-kv-engine/blob/a2801cff1a43bab8ef6f5c35344a388c9e8c75c6/kv-engine/src/wal.rs#L749-L756), [Parallel handles](https://github.com/ben1009/toy-kv-engine/blob/a2801cff1a43bab8ef6f5c35344a388c9e8c75c6/kv-engine/src/wal.rs#L1459-L1494)). V8 Parallel opens `write_file` with ordinary buffered I/O, clones it as `sync_file`, and separately attempts to open the DATA worker with `O_DIRECT`; that worker can fall back to buffered I/O on filesystems that reject direct I/O ([V8 attachment](https://github.com/ben1009/toy-kv-engine/blob/a2801cff1a43bab8ef6f5c35344a388c9e8c75c6/kv-engine/src/mem_table.rs#L1817-L1837), [worker fallback](https://github.com/ben1009/toy-kv-engine/blob/a2801cff1a43bab8ef6f5c35344a388c9e8c75c6/kv-engine/src/mem_table.rs#L56-L95)). The benchmark results do not record whether the V8 DATA worker actually opened with `O_DIRECT`. The captured stderr files are empty, so these runs do not establish the DATA I/O mode; buffered DATA remains a confound in comparing FRONTIER behavior.

`WalV8ParallelWriter::drive_frontier_through()` writes FRONTIER through `write_file`, then calls `fdatasync` through `sync_file` ([V8 frontier and sync](https://github.com/ben1009/toy-kv-engine/blob/a2801cff1a43bab8ef6f5c35344a388c9e8c75c6/kv-engine/src/wal/v8/leader.rs#L1671-L1815)). Buffered FRONTIER pages are a plausible contributor to the observed sync latency if the DATA worker used `O_DIRECT`. This source-level difference is a hypothesis, not a proven cause; the actual DATA mode in these runs is unknown, and filesystem writeback, allocation, and other state can also affect syscall latency.

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

First instrument or require the DATA worker's actual I/O mode, then run a controlled V8 Parallel A/B with the same confirmed DATA mode in both arms. Change only how FRONTIER is written: current buffered FRONTIER versus an aligned `O_DIRECT` FRONTIER write. Keep DATA, digest, ticket, group commit, the sync handle, and one-`fdatasync` durability semantics unchanged. Do not only replace `sync_file`; the variable to test is the FRONTIER write itself. Measure per-run throughput, syscall latency distribution, sync count, p50, and p99 on the same host in counterbalanced order.

The experiment must preserve DATA completion → FRONTIER write → `fdatasync` → durable publication ordering, and verify recovery at the `wal_v8.parallel.after_frontier_write`, `wal_v8.parallel.after_fdatasync`, and `wal_v8.parallel.after_durable_publish` crash points. If direct FRONTIER I/O does not reduce sync latency, then correlate per-sync latency with DATA completion, FRONTIER timing, file descriptor/inode, dirty bytes, and ticket readiness before changing write order/coalescing or adding a small-transaction fast path. These measurements do not justify changing BLAKE3 or rewriting the ticket queue.

## Raw artifacts

The raw run archive is retained locally and is not included in this PR. It contains per-run logs, command lines, and host metadata; the report includes the aggregate measurements and setup details used for this analysis. No repository source files were modified.
