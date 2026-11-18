# RFC 026 V8 Point Lookahead with Buffered vs Direct FRONTIER on GCP — 2026-11-17

## Run identity

- Source commit: `398e8a0aee17993b13a7685161389af3b3408564`
- Benchmark binary SHA-256: `8f5b59831b4b0dbb052cc7ce6bcf505f376d46683d85596f86c7ace310ddd223`
- Source archive SHA-256: `04053ee47477256807822b952aebf6625dc3f1d0197051b8641038d7d034148f`
- Workload: 16 writers, 100,000 Point puts per run, 1 KiB values
- Hardware and storage: GCP C4 `c4-standard-16-lssd`, two Local SSDs in RAID0, ext4
- Kernel: `7.0.0-1017-gcp`; mount options: `rw,noatime,stripe=256`
- Score runs: 10/10 completed; five counterbalanced pairs, in AB/BA/AB/BA/AB order
- Latency sampling: every 100 puts; no detailed profiling or eBPF tracing

## Setup and method

Both arms used V8 Parallel, 1 MiB Point-only Sliding Lookahead, and Dynamic coalescing. The coalescing override was unset. Only the FRONTIER mode changed:

- **A:** Buffered FRONTIER / Dynamic coalescing
- **B:** Direct FRONTIER / Dynamic coalescing

The benchmark validator confirmed the expected extent preparation mode and FRONTIER descriptor mode at both WAL I/O boundaries for every run. The workload used fresh database paths for each run.

## Score runs

Values are medians across five runs. Latency percentiles are sampled by the benchmark.

| Metric | Buffered / Dynamic | Direct / Dynamic |
|---|---:|---:|
| Throughput | 38,654 puts/s | 41,796 puts/s |
| CPU | 80.51 μs/put | 78.98 μs/put |
| Put p50 | 0.360 ms | 0.335 ms |
| Put p95 (sampled) | 0.965 ms | 0.689 ms |
| Put p99 (sampled) | 1.887 ms | 1.341 ms |
| Sync count | 9,726 | 10,062 |
| Puts / Sync | 10.28 | 9.94 |
| Initialized extent bytes | 408.9 MB | 407.9 MB |
| Preparation wait | 53.88 ms | 53.46 ms |

Paired throughput changes for Direct vs Buffered were `+11.30%`, `+4.83%`, `+9.68%`, `+8.33%`, and `+5.85%`. The median paired change was **+8.32%**, with Direct winning all five pairs.

Direct FRONTIER did not improve throughput by reducing Sync frequency or enlarging groups: its median Sync count was about 3.5% higher, and Puts / Sync about 3.3% lower. CPU per put was about 1.9% lower. The observed gain is consistent with a faster FRONTIER durability cycle, though this A/B does not isolate a specific kernel mechanism.

The p95 and p99 medians also moved down, but they are based on one-in-100 latency samples and five runs. Treat those tail changes as directional, not as qualified tail-latency improvements. This experiment did not include a V7 control, Batch workloads, or other writer counts, so it does not establish a production default or quantify the remaining V7/V8 gap.

## Conclusion

Direct FRONTIER is a promising candidate when combined with Point-only Lookahead: it improved throughput in all five paired runs without increasing CPU per put. Keep it as a candidate for broader qualification; this result alone does not justify changing the production default.

The local result archive passed SHA-256 verification and contains all 10 successful run records and matching metadata. Its SHA-256 is `a087600816819bc99ab191101f7098e45337f5acd51cc9427a32873deecbe676`. The archive is retained locally and is not included in this PR.

Cleanup verification confirmed the VM, temporary SSH firewall, isolated VPC, subnet, and egress route are absent; the temporary Terraform state is empty.
