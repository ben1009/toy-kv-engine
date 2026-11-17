# RFC 026 V7/V8 Point FRONTIER Qualification Rerun on GCP — 2026-11-17

## Run identity

- Source commit: `dc067676fa4205e93ae98ea9e74cecb876e7506d`
- Benchmark binary SHA-256: `c0ffb35651bb032468a3b8e3941a3e59dc7785f8b87cb328dd9f319bb31c8535`
- Source archive SHA-256: `32e136d9b83d1e65dee1bd0ba88711569f582f64c51d9e2cd20e64734c1e860b`
- Temporary qualification runner SHA-256: `9a7b4fd4fb3223ffff12b28e1426fde666ce29b473b21bab3f55700f08d2570a`
- Workload: 16 writers, 100,000 Point puts per run, 1 KiB values
- Score runs: 15/15 completed; five counterbalanced repetitions per arm
- Latency sampling: every 100 puts (1,008 samples per run); no profiling, eBPF, or WAL diagnostic instrumentation
- Hardware and storage: GCP C4 `c4-standard-16-lssd`, two Local SSDs in RAID0, ext4
- Kernel: `7.0.0-1017-gcp`; mount options: `rw,noatime,stripe=256`
- CPU: Intel Xeon 6985P-C, 16 logical CPUs / 8 cores
- Rust: `1.100.0-nightly` (`nightly-2026-09-23`)

## Setup and method

All arms ran on the same VM with fresh database paths. V8 used 1 MiB Point-only Sliding Lookahead, Dynamic coalescing (`TOYKV_WAL_V8_COALESCE_BUDGET_US` unset), and O_DIRECT DATA. The two V8 arms differed only in FRONTIER descriptor mode.

- **V7:** Parallel WAL, native V7 behavior
- **V8 Buffered:** Point-only Lookahead, Buffered FRONTIER, Dynamic coalescing
- **V8 Direct:** Point-only Lookahead, Direct FRONTIER, Dynamic coalescing

The result validator checked WAL format, DATA and FRONTIER descriptor modes, lookahead mode and window at both I/O boundaries, and absence of diagnostic instrumentation for each run.

## Score runs

Median across five runs per arm. Latency percentiles are based on the benchmark's one-in-100 operation samples.

| Arm | Throughput | CPU/put | p50 | sampled p95 | sampled p99 | Syncs | Puts/sync |
|---|---:|---:|---:|---:|---:|---:|---:|
| V7 Parallel | 30.40k/s | 60.48 μs | 0.378 ms | 1.720 ms | 2.555 ms | 8,694 | 11.50 |
| V8 Lookahead / Buffered / Dynamic | 31.12k/s | 79.20 μs | 0.425 ms | 1.629 ms | 3.395 ms | 9,451 | 10.58 |
| V8 Lookahead / Direct / Dynamic | 30.73k/s | 78.76 μs | 0.406 ms | 1.508 ms | 2.506 ms | 10,099 | 9.90 |

Per-repetition throughput in k puts/s and Direct-vs-Buffered change:

| Repetition | V7 | V8 Buffered | V8 Direct | Direct vs Buffered |
|---:|---:|---:|---:|---:|
| 1 | 25.387 | 30.599 | 30.585 | −0.05% |
| 2 | 30.433 | 31.204 | 30.972 | −0.74% |
| 3 | 30.388 | 31.106 | 30.577 | −1.70% |
| 4 | 30.434 | 31.382 | 30.727 | −2.09% |
| 5 | 30.403 | 31.124 | 30.865 | −0.83% |

The median paired throughput change for Direct vs Buffered was **−0.83%**, with Direct winning **0/5** pairs. This rerun does not reproduce the previous +8.32% Direct FRONTIER gain. Direct had slightly lower CPU/put and lower sampled latency percentiles, but it performed more syncs; the throughput result does not show an end-to-end gain.

V8's throughput was numerically higher than the matching V7 run in all five blocks. Excluding the first block, where V7 was a low outlier at 25.387k/s, the paired median differences were +2.45% for Buffered and +1.24% for Direct. The other four V7 runs were tightly grouped at 30.388–30.434k/s, while the V7 median of 30.40k/s is far below the earlier 44–48k/s captures. This single capture therefore does not establish a stable V7/V8 performance advantage or gap.

## FRONTIER I/O counters

Median of each run's aggregate time divided by its sync/write count:

| Arm | `fdatasync` per sync | FRONTIER write per sync |
|---|---:|---:|
| V7 Parallel | 39.49 μs | 101.95 μs |
| V8 Buffered / Dynamic | 85.38 μs | 38.21 μs |
| V8 Direct / Dynamic | 35.91 μs | 81.49 μs |

Direct shifted time from `fdatasync` into FRONTIER writes, but this did not translate into higher throughput in this capture. Its median sync count was 6.9% higher than Buffered, so the result also does not reflect larger durability groups.

## Validation and result retention

The first V7 run produced a successful 100,000-put result. The initial temporary runner misread the integer `latency.samples` field as an array and rejected that record during validation. After correcting the validator, the same raw result was revalidated and reused; the other 14 runs then completed. No measurement was repeated or omitted. The original validation output and both runner versions are retained with the local results.

The complete result archive and SHA-256 sidecar were verified locally. Archive SHA-256: `e167f77b10c655835da0630e4ac2d99ebcc98345aa6fe4b8bfe0b399ce81bd93`. The archive and per-run JSON remain local and are not included in this PR; this report contains the aggregate and per-repetition score results.

After result verification, Terraform destroyed the VM, boot disk, temporary SSH firewall, isolated VPC and subnet. GCP inventory confirmed those resources and the VPC routes were absent. Temporary credentials, Terraform state, and plan files were removed.

## Conclusion

This capture does not confirm a Direct FRONTIER throughput advantage over Buffered FRONTIER. Keep the production default unchanged. The unusually low V7 baseline means this run also cannot qualify the remaining V7/V8 performance gap; treat it as a non-replication of the previous Direct gain and evidence of substantial between-capture variation.
