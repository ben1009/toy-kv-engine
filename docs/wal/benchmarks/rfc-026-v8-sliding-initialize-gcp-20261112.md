# RFC 026: V8 Sliding Extent Initialization on GCP (2026-11-12)

Status: benchmark-only experiment. Background zero initialization did not show a throughput benefit over sliding `fallocate` and is not qualified for production use.

## Question

Does initializing each sliding, preallocated V8 WAL window in the background reduce durability cost enough to improve Point-write throughput?

## Setup

- GCP `c4-standard-16-lssd`, two Local SSDs in the repository's ext4 RAID0 test setup.
- 16 writers; 100,000 Point puts per run; 1 KiB values.
- Five runs per arm, interleaved in one session. No `--profile` or per-sync diagnostics; latency was sampled once per 100 puts. Aggregate benchmark counters remained enabled.
- V7 used Parallel WAL. Both V8 arms used Direct FRONTIER and a fixed 400 μs coalescing budget.
- Sliding preparation used 1 MiB windows. The initialize arm zero-wrote aligned 128 KiB chunks on a background thread. Preparation ran during measured writes.
- Every V8 run verified DATA `O_DIRECT=true`, FRONTIER `O_DIRECT=true`, Sync `O_DIRECT=false`, and all three handles on the same inode.

One initial runner attempt completed a V7 workload but rejected its result because the V7 writer does not export the V8 handle-stability diagnostic. The check was scoped to V8 and the full 15-run matrix was then repeated; the rejected run is excluded from the table. Because it ran on the same VM first, it may have affected device/cache state, so treat this as a diagnostic comparison rather than a formal qualification.

## Results

Throughput is the median across five runs; the range shows the five individual run values. Latency columns show the median of each run's sampled percentile. CPU and I/O values are likewise medians across runs.

| Arm | Throughput (puts/s) | p50 (ms) | p95 (ms) | sampled p99 (ms) | CPU (s) | Syncs | mean `fdatasync` (μs) | FRONTIER write + Sync (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| V7 Parallel | 48,943 (48,852–49,243) | 0.299 | 0.552 | 0.821 | 5.893 | 8,220 | 15.46 | 422.4 |
| V8 sliding `fallocate` | 34,095 (33,950–34,779) | 0.467 | 0.611 | 1.121 | 6.923 | 6,336 | 96.79 | 860.7 |
| V8 sliding initialize | 34,211 (34,191–34,264) | 0.473 | 0.672 | 2.469 | 7.195 | 6,328 | 53.23 | 568.1 |

Sliding initialization's median throughput was 0.34% above `fallocate` alone. The run ranges overlap, so this is not evidence of a repeatable throughput gain. V8 reached about 69.9% of the V7 median in this run set.

Initialization changed the durability counters: the per-run mean `fdatasync` fell from 96.79 μs to 53.23 μs, and cumulative FRONTIER write plus Sync time fell from 860.7 ms to 568.1 ms. Throughput remained essentially flat, while median process CPU increased by 0.27 seconds. The background thread initialized a median 415.4 MiB per run in 3,323 chunks; producers accumulated 343 ms of preparation wait across threads. Those waits overlap and are not wall-clock duration.

The sampled p99 median was higher with initialization (2.469 ms versus 1.121 ms), with run-to-run variation. These samples are diagnostic and do not establish a stable tail-latency regression. The result does show that the lower Sync cost did not translate into measured throughput improvement under this workload; additional zero-write and coordination costs are plausible offsets, but this run does not establish their causal contributions.

## Conclusion

Keep sliding `fallocate` as the extent-preparation candidate. Do not enable background zero initialization by default: it made Sync measurements better but did not improve Point throughput in this controlled run set, and its sampled p99 warrants more investigation before any latency claim.

This run did not collect eBPF `-EAGAIN`/io-wq traces or the guest kernel version, so it cannot confirm how zero initialization changed the kernel submission path. It also covers only 16-writer Point writes; it is not a performance qualification across writer counts or workloads.

## Validation and provenance

- The sliding initializer's concurrent multi-window unit test passed.
- The focused V8 durability-boundary crash-recovery matrix passed, including sliding initialization with both FRONTIER modes and all existing crash points. Recovery trimmed prepared suffixes back to the recovered image length.
- `cargo check --package kv-engine --features bench --bin write-perf`, `cargo check --package kv-engine --lib`, and targeted Clippy passed.
- Benchmark base commit: `40ffd183c2bdf3fb64693dbb3756c0b3a15bddcf`.
- Experimental source patch SHA-256: `97e2801c319e466bf243321719ff0fe7541c4755b1945c3ed7200f5c01728e3a`.
- Temporary runner SHA-256: `2d9c7187423477049f961f5a8919967a2831a55e37906983e9c62f8188a9c912`.
- GCP cleanup was verified: VM, auto-delete boot disk, SSH firewall, subnet, VPC, and VPC routes are absent.
- Per the benchmark workflow, only this summary is retained in the repository; raw result files remain local and no archive was uploaded.
