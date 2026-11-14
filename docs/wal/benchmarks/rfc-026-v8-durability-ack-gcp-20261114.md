# GCP V8 durability and ACK diagnostic

- Run: 16 Writers, 100,000 Point puts, 1 KiB values; V8 Parallel, Direct FRONTIER, fixed 400 μs coalescing, Sliding Fallocate 4 MiB.
- This is one instrumented diagnostic run, not a throughput qualification or repeated A/B. The durability sampler is every 64 completed advances; no perf capture or broad `--profile` was used.
- Source base: `6c668dda79081517d4e96d3d41f55fecd3474f04`; patch SHA-256: `dfd4c68875aff2a6a63d0f7c994bcfa20ad14a00c246928b7f4f8c5c203186eb`.
- Archive SHA-256 was verified locally before extraction. `summary.json` is complete (1/1); `remote-exit.json` reports success (1 started); the planned run has both `result.json` and matching `run-metadata.json`.

## Result

| Metric | Value |
| --- | ---: |
| Throughput (diagnostic) | 33.20k puts/s |
| Operations | 100,000 |
| Sampled p99 | 0.760 ms |
| Process CPU | 7.660 s |
| Sync count | 6,330 |
| DATA / FRONTIER / Sync `O_DIRECT` | True / True / False |
| Completed / sampled / dropped advances | 6330 / 98 / 0 |
| ACK samples / dropped | 1,562 / 0 |

## Sampled phase distributions

Values are p50 / p95 across 98 sampled successful advances (μs); intervals overlap and should not be added. ACK values are p50 / p95 across 1,562 sampled puts (μs).

| Phase | p50 / p95 (μs) |
| --- | ---: |
| Driver wall | 418.9 / 553.4 |
| Driver thread CPU | 162.7 / 207.0 |
| Coalescing wall | 169.1 / 280.6 |
| Coalescing thread CPU | 51.3 / 92.5 |
|   Condvar wait + reacquire wall | 159.9 / 275.7 |
|   Condvar wait + reacquire thread CPU | 44.6 / 82.6 |
| Coalescing active thread CPU | 6.8 / 14.4 |
| Retirement before publish wall | 36.2 / 39.5 |
| Retirement before publish thread CPU | 36.1 / 39.4 |
|   Before-publish lock wait wall | 0.4 / 0.7 |
|   Before-publish lock hold wall | 34.8 / 38.1 |
| Retirement after publish wall | 43.5 / 85.3 |
| Retirement after publish thread CPU | 33.6 / 51.7 |
|   After-publish lock wait wall | 0.3 / 0.4 |
|   After-publish lock hold wall | 42.4 / 84.3 |
| FRONTIER write wall | 39.8 / 55.5 |
| fdatasync wall | 101.8 / 119.6 |
| Coordinator publish wall | 4.5 / 5.2 |
| Durable publish → WAL return | 30.6 / 64.4 |
| WAL return → MemTable publish start | 2.4 / 4.7 |
| MVCC commit timestamp publication | 20.0 / 103.0 |
| WAL return → Put internal return | 26.9 / 109.1 |

Coalescing return counts per sampled advance (p50): waits=4, returns=4, prefix-growth returns=4, generation-change returns=4, timeout returns=0.

## Interpretation

The ticket-correlated ACK path is now measured end to end in the benchmark output. The sampled results separate the WAL wait from post-WAL MemTable/MVCC publication; they do not by themselves establish which stage causes the V7/V8 throughput gap. This single run is useful for locating the next experiment, not for claiming a stable performance change.

The retained VM and Terraform network were not manually deleted. The VM remains subject to its already configured 60-minute server-side deadline. The prior incomplete collection directory and archives were left untouched.


## Sampler overhead control

A counterbalanced sampler-off/on ABBA follow-up was prepared but not started: the GCP runner's safety check found only 1.8 minutes remaining before the VM's existing 60-minute server-side deletion deadline. I did not extend the VM lifetime or provision another VM. The verified sampled run above is complete; instrumentation overhead remains unquantified by this run.

## Review follow-up

Review found that an ACK timestamp evicted from the bounded map could increment `ack_dropped_samples` both at eviction and when its writer later observed the missing timestamp. The eviction-side increment was removed; the missing timestamp is counted once when the sampled writer consumes it. This run reported zero dropped samples, so the correction does not change its measurements. The source commit and patch checksum above identify the exact code measured by GCP; this counter-only correction was applied afterward.
