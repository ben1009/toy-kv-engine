# GCP V7/V8 WAL comparison

## Run

- Source commit: `437f58b18f40b68dd098b49ae9539da4a4d7c861`
- Machine: C4 standard 16, 16 vCPUs, 8 cores with SMT, two NVMe Local SSDs in RAID0.
- Filesystem: ext4, mounted `rw,noatime,stripe=256`.
- Kernel: `7.0.0-1017-gcp`.
- Workload: 16 writers, 1 KiB values; Point, Batch8, Batch16, Batch64; five repetitions per configuration.
- 60 runs total. Profiling and per-sync diagnostics were off; aggregate benchmark counters and latency sampling remained enabled.
- V8 used sliding 1 MiB fallocate. The Direct arm used Direct FRONTIER and a 400 µs coalescing override; the Buffered arm used dynamic coalescing.

## Throughput

Throughput is the median across five runs. Paired deltas match repetitions.

| Workload | V7 puts/s | V8 Buffered puts/s | V8 Direct/400 puts/s | Direct/400 vs V7 (paired median) | Direct/400 vs Buffered (paired median) |
| --- | ---: | ---: | ---: | ---: | ---: |
| point | 49,521 | 32,687 | 34,664 | -30.39% (0/5 wins) | +6.12% (5/5 wins) |
| batch8 | 215,722 | 216,268 | 228,486 | +5.80% (5/5 wins) | +5.65% (5/5 wins) |
| batch16 | 261,969 | 377,735 | 392,597 | +49.87% (5/5 wins) | +4.47% (5/5 wins) |
| batch64 | 618,102 | 620,165 | 619,257 | +0.11% (5/5 wins) | -0.18% (0/5 wins) |

## Latency and sync counts

Median of the five per-run measurements; p99 is based on sampled operation latencies.

| Workload | Arm | p50 ms | p95 ms | sampled p99 ms | mean sync count |
| --- | --- | ---: | ---: | ---: | ---: |
| point | V7 Parallel | 0.294 | 0.519 | 0.801 | 8,203.8 |
| point | V8 Sliding / Buffered / Dynamic | 0.487 | 0.614 | 0.968 | 6,341.8 |
| point | V8 Sliding / Direct / 400 µs | 0.454 | 0.586 | 0.745 | 6,331.0 |
| batch8 | V7 Parallel | 0.534 | 1.180 | 1.440 | 4,358.0 |
| batch8 | V8 Sliding / Buffered / Dynamic | 0.586 | 0.719 | 0.815 | 2,365.6 |
| batch8 | V8 Sliding / Direct / 400 µs | 0.553 | 0.689 | 0.779 | 2,362.4 |
| batch16 | V7 Parallel | 0.713 | 2.038 | 2.331 | 3,873.2 |
| batch16 | V8 Sliding / Buffered / Dynamic | 0.672 | 0.813 | 1.034 | 1,970.0 |
| batch16 | V8 Sliding / Direct / 400 µs | 0.642 | 0.779 | 0.879 | 1,969.2 |
| batch64 | V7 Parallel | 1.471 | 2.672 | 2.918 | 1,217.2 |
| batch64 | V8 Sliding / Buffered / Dynamic | 1.598 | 2.115 | 2.453 | 1,379.4 |
| batch64 | V8 Sliding / Direct / 400 µs | 1.574 | 2.267 | 2.652 | 1,404.8 |

## Readout

- Point: Direct/400 improved 6.12% over V8 Buffered/Dynamic in all five pairs, but remained 30.39% below V7. V8 used fewer syncs than V7, so sync frequency alone does not explain the remaining Point gap.
- Batch8: Direct/400 was 5.80% above V7 and 5.65% above Buffered in all five pairs.
- Batch16: Direct/400 was 49.87% above V7 and 4.47% above Buffered in all five pairs.
- Batch64: Direct/400 was effectively tied with V7 (+0.11% paired median); it was 0.18% below Buffered and lost all five pairs. This difference is too small to treat as a meaningful regression.
- Sampled p99 values are descriptive only; this run does not establish stable tail-latency behavior or explain the remaining Point gap causally.

## Artifact verification

- Summary: `60/60` runs completed; matrix validation observed 60 unique valid rows and no failures.
- Local files: 60 `result.json` and 60 `run-metadata.json` files.
- Transferred bundle: SHA-256 `4380ad3b06ab530dbf0f06378ac4b6e06dca909805ada30dfeaa9251ed4ac60a`; 372 files, 53958 bytes. Remote and local bundle checksums matched before extraction.
- The VM and SSH rule were retained by the runner; Terraform network and state remain. The VM has a 25-minute server-side deletion deadline.
- The spend estimate for the failed first VM plus this rerun is about $1.95 against the $2 guard. This is an estimate, not a hard billing cap.

The per-run JSON and transferred archive remain local under the benchmark result directory. This Markdown summary is tracked here; no archive was uploaded to GitHub.
