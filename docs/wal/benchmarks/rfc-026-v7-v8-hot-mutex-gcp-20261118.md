# GCP V7/V8 hot-mutex diagnostic

## Run identity

- Source commit: `65cf15e49f9d59d7faa54ed3751106b2c84f5eb3`
- Binary SHA-256: `629b80c190a14099c6ad66005d6eaf9c29eb49313049e6c03eb34263a1849c8d`
- Machine: C4 16 vCPU with 2 Local SSDs, ext4 RAID0; kernel `7.0.0-1017-gcp`; mount `rw,noatime,stripe=256`.
- Workload: 16 writers, 100,000 1 KiB Point puts, 5 counterbalanced V7/V8 pairs; lock sampling every 64 acquisitions.
- V8 arm: Point-only Lookahead, 1 MiB window, Direct FRONTIER, dynamic coalescing. The diagnostic intentionally does not instrument the Coordinator Mutex.
- This is an instrumented diagnostic, not an uninstrumented throughput qualification.

## Outcome

| Metric | V7 Parallel | V8 Point Lookahead + Direct | Paired result |
|---|---:|---:|---:|
| Throughput (puts/s) | 47,806.44 | 40,341.42 | -15.95% |
| Process CPU (μs/put) | 60.68 | 81.42 | +34.16% |
| Sync count | 8,298.00 | 10,142.00 | — |
| Puts/sync | 12.05 | 9.86 | — |
| Put p50 (ms) | 0.30 | 0.35 | +16.97% |
| Put p95 sampled (ms) | 0.56 | 0.77 | +40.14% |
| Put p99 sampled (ms) | 0.78 | 1.21 | +66.38% |

V8 was slower in all five pairs. Its median process CPU was about 20.74 μs/put higher (+34.2%). These CPU and throughput values include the diagnostic instrumentation.

## Per-mutex measurements

All values below are medians across the five runs for each arm. Counts: exact acquisitions / sampled acquisitions / sampled contended / estimated contended. Wait and hold quantiles are log2 histogram bucket upper bounds in ns; cumulative wait/hold are sample-scaled estimates across the 100k-put run.

| Mutex | V7 counts | V7 wait p50/p95/p99; total | V7 hold p50/p95/p99; total | V8 counts | V8 wait p50/p95/p99; total | V8 hold p50/p95/p99; total |
|---|---:|---:|---:|---:|---:|---:|
| resource_ledger | 391,702 / 6,109 / 0 / 0 | 64/256/512 ns; 22.05 ms | 128/512/1024 ns; 110.68 ms | 499,500 / 7,795 / 2 / 128 | 64/256/512 ns; 37.22 ms | 256/1024/2048 ns; 135.13 ms |
| reservation_owner | 302,553 / 4,716 / 177 / 11,355 | 64/512/2048 ns; 119.17 ms | 1024/2048/4096 ns; 216.59 ms | 399,500 / 6,233 / 176 / 11,278 | 64/512/1024 ns; 37.41 ms | 512/2048/4096 ns; 200.79 ms |
| v8_extent_state | — | — | — | 534,779 / 8,347 / 483 / 30,945 | 64/512/2048 ns; 81.07 ms | 512/2048/4096 ns; 239.98 ms |
| v8_extent_preparation | — | — | — | 98,778 / 1,535 / 2 / 128 | 64/256/512 ns; 7.40 ms | 128/512/2048 ns; 279.44 ms |
| mvcc_publication | 164,224 / 2,560 / 929 / 59,638 | 128/32768/65536 ns; 442.81 ms | 512/4096/8192 ns; 118.92 ms | 172,425 / 2,687 / 1,272 / 81,683 | 256/8192/65536 ns; 345.32 ms | 512/2048/8192 ns; 91.36 ms |

Across mutexes instrumented in each implementation, the median per-run sum of estimated cumulative wait was 580.08 ms for V7 and 506.90 ms for V8; the median paired change was −83.26 ms per run. This is cumulative thread wait, not wall-clock benchmark time. V8-only Extent State and Extent Preparation locks contributed estimated waits of 81.07 ms and 7.40 ms respectively.

The estimated aggregate hold time was higher for V8 (median 942.06 ms vs 446.28 ms; paired median +524.19 ms). Hold time is elapsed lock-held time, not thread CPU, and summing lock classes can overlap across threads. It indicates more/longer critical-section activity, not proof that mutex waiting consumed the extra CPU.

## Interpretation and limits

- The measured mutex wait path does not explain the V8 CPU/put increase: overall estimated wait is lower in V8, and the V8-only extent-state wait is under 1 μs/put by the estimate. MVCC publication has more estimated contended acquisitions, but lower estimated total wait and a lower p95 wait bucket than V7; p99 is the same bucket.
- V8 has substantially more instrumented acquisitions and higher aggregate hold-wall time. This can indicate additional work in critical sections, but the counters do not measure per-lock thread CPU and cannot attribute the +20.74 μs/put.
- Exact acquisition counters are updated on every instrumented acquisition. Sampling reduces timer/histogram work to 1/64, but this per-call counter work and the extra V8 lock classes can affect CPU/throughput. Do not treat diagnostic CPU numbers as clean performance scores.
- The Coordinator Mutex was intentionally left untouched. Extent locks exist only in V8, so direct V7/V8 comparisons apply to Resource Ledger, Reservation Owner, and MVCC Publication only.

## Artifact verification

- Official artifact collector validated `summary.json` (`complete=true`, 10/10), `remote-exit.json` (`failed=false`, 10 runs), and every scheduled run ID had a successful `result.json` paired with matching `run-metadata.json`.
- The raw result archive and SHA-256 sidecar are retained locally under `bench-results/gcp-v8-hot-mutex-diagnostic-65cf15e4-recovered/collection-attempt-01/`; they are not committed. This repository contains the summary report only.
- The first wrapper invocation stopped before benchmarking because GitHub rejected fetching the raw SHA as an unadvertised ref. The same retained VM was reused; the existing PR branch ref was fetched, checked out and verified against the exact commit before building/running. This is recorded in `resume-method.json`.
- VM, temporary SSH firewall, isolated VPC and subnet remain present. No cloud resource was deleted; the VM keeps its configured 60-minute server-side maximum lifetime.
