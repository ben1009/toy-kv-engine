# GCP V8 Durability Sampling

## Run

- Source commit: `873caefb280fd1f66b27af58671db557a7b67c6d` plus the recorded sampler fix patch (`dcc0947246477e78e7056fe1b8f9255cf04be084296f2ac0a96456438868194e`).
- Machine: `c4-standard-16-lssd`, zone `us-east1-b`; Linux `7.0.0-1017-gcp`; ext4 on `/mnt/gcp-wal-bench`, mount options `rw,noatime,stripe=256`.
- Workload: Point, 100,000 puts per run, 1 KiB values; V8 Parallel, Direct FRONTIER, fixed 400 μs coalescing, Sliding Fallocate 4 MiB.
- Four single diagnostic runs at 1, 4, 8, and 16 writers. No `perf` or Hotpath capture. Runtime durability phases sampled every 64 driver attempts; client latency sampled every 100 operations.
- These are diagnostic runs with measurement overhead, not throughput qualification; there is no matched V7 arm or repetition at each writer count.

## Results

Latency values are milliseconds. CPU is process CPU seconds.

| Writers | Throughput | p50 | p95 | p99 (sampled) | Process CPU | Syncs | Driver phase samples | Follower samples |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4.38k | 0.228 | 0.258 | 0.285 | 6.49 | 100,000 | 3,125 | 0 |
| 4 | 14.12k | 0.277 | 0.338 | 0.388 | 4.73 | 25,037 | 281 | 1,171 |
| 8 | 23.70k | 0.334 | 0.405 | 0.826 | 5.53 | 12,557 | 158 | 1,368 |
| 16 | 34.01k | 0.465 | 0.597 | 0.815 | 7.14 | 6,345 | 105 | 1,465 |

All four runs completed 100,000 puts and reported stable WAL handle identity. DATA and FRONTIER were `O_DIRECT`; the sync handle was buffered. Each run reported sliding-fallocate progress.

## 16-writer sampled phases

Values are sample p50/p95 in microseconds, except counts. Composite percentiles are computed from each sample's combined duration rather than by adding marginal percentiles.

| Metric | p50 | p95 |
| --- | ---: | ---: |
| Tickets covered per advance | 16 | 16 |
| Driver wall | 366.2 | 480.9 |
| Driver thread CPU | 112.3 | 147.0 |
| Coalescing wall | 156.2 | 243.1 |
| Coalescing thread CPU | 44.0 | 70.7 |
| FRONTIER write wall | 36.8 | 53.1 |
| FRONTIER write thread CPU | 8.2 | 12.9 |
| fdatasync wall | 94.5 | 109.0 |
| fdatasync thread CPU | 9.0 | 10.9 |
| FRONTIER write + fdatasync wall | 134.4 | 151.1 |
| Resource retirement wall (before + after publish) | 46.9 | 84.9 |
| Coordinator publish lock wait | 0.2 | 0.3 |
| Coordinator publish wall | 4.5 | 5.1 |
| notify_all wall | 2.0 | 2.2 |
| Followers waiting at publish | 15 | 15 |

| Follower metric | p50 | p95 |
| --- | ---: | ---: |
| Follower total wait wall | 346.3 | 465.1 |
| Follower total wait thread CPU | 4.1 | 10.6 |
| Follower coordinator lock wait | 0.5 | 1.3 |
| notify to follower resume | 22.6 | 56.4 |
| Followers woken by last notify | 15 | 15 |

## What this says

- `fdatasync` consumed about 9 μs of thread CPU at p50 while taking about 95 μs wall time. The syscall interval is mostly waiting, not CPU execution.
- At 16 writers, sampled advances covered 16 tickets. Coalescing plus FRONTIER write and sync had a 285 μs p50; the full driver interval was 366 μs p50 and 481 μs p95.
- Publish lock wait stayed below 1.3 μs at p95, and publish itself was about 4.5 μs p50. These samples do not support Coordinator mutex contention as a large part of the cycle.
- Followers waited 346 μs p50 with about 4 μs thread CPU. The last notify woke 15 followers at p50; notify-to-resume was 23 μs p50 and 56 μs p95. This captures follower wait and wake timing, not durable-publish-to-`put()` return or MVCC publication.
- Resource-retirement wall time (before and after publication combined) was 47 μs p50 and 85 μs p95 in the sampled 16-writer advances. It is a follow-up candidate, but this run does not isolate its causal impact.

## Verification and retained artifacts

The runner summary reports `complete: true` with 4/4 runs. `remote-exit.json` reports `failed: false` and 4 starts. The downloaded archive checksum passed: `297501ab14d23b7c034a6b2dde424e8a35ad88c11787f7d15f26024edb4a932b`. The archive and checksum metadata are retained locally; the VM/network were left intact.

The initial run bundle remains separately preserved. Its workloads completed, but an overly strict validator rejected expected sliding-fallocate progress changes, and the original sampler did not advance its sequence, so it contained no driver phase samples. The corrected run above uses the recorded sampler and runner patches. The sampler fix was subsequently committed and pushed as `25ceb3c6`; the runner-validator patch remains local and was not included in that push.
