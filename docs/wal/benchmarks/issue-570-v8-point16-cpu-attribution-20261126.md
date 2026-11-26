# Issue #570 Point16 CPU Attribution Breakdown (From Retained GCP Archive)

- **Date**: 2026-11-26
- **Issue**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570)
- **Source Archive**: `bench-results/gcp-issue570-ab-20261126/issue570-ab/diag-point16-{baseline,candidate}/result.json` (`sha256` of `issue570-ab.tar.gz`: `a17e4e2bbbe0eed46fafd23a3c548f328892ef08e896eef51602fc22f16b6939`)
- **Environment**: `c4-standard-16-lssd` (`us-central1-b`), 16 writers, 100,000 puts, 1 KiB values, Candidate Target8 default (`30c9092d`), `--wal-v8-durability-sample-every 64` (no new GCP run; extracted from retained archive)

## 1. High-Level Point16 CPU Budget

- **Unprofiled Point16 median CPU/Put** (40-run A/B): `74.75 μs` (baseline `20d83e81`) → **`70.92 μs`** (candidate `30c9092d`, `-5.78%`).
- **Diagnostic Point16 total CPU/Put** (`--wal-v8-durability-sample-every 64`): `79.23 μs` (baseline, `11,943` syncs, `8.37` ops/sync) → **`73.33 μs`** (candidate, `11,961` syncs, `8.36` ops/sync).
- **Target**: `<= 60.00 μs/put` (`~10.9 μs/put` remaining gap from `70.92 μs`).

| Coarse Bucket (Candidate Diagnostic) | Mean CPU (μs/put) | Share of 73.33 μs | Notes |
| --- | ---: | ---: | --- |
| **Driver Advance (`driver_cpu_ns`)** | 10.952 | 14.9% | Covers coalescing, prep, FRONTIER write/sync, publish, and retirement |
| **Follower Wait (`total_wait_cpu_ns`, ×0.880)** | 9.790 | 13.4% | Down from `11.912 μs/put` in baseline (`-2.12 μs/put`) |
| **Other / Non-Driver-Wait** | ~52.59 | 71.7% | Producer admission/encode/submit, io_uring DATA worker, MVCC, SkipList |

## 2. Detailed Driver & Follower Breakdown (Candidate `30c9092d`, μs/put)

Ranked by mean CPU/Put (`1,562` sampled advances, `8.36` ops/sync):

| Rank | Stage / Sub-phase | Priority | Median (μs/put) | Mean (μs/put) | Baseline Mean (μs/put) |
| ---: | --- | --- | ---: | ---: | ---: |
| 1 | **Follower Wait (`total_wait_cpu_ns`, weighted)** | P0 | 8.959 | **9.790** | 11.912 |
| — | └ Follower coordinator lock wait | P0 | — | 0.652 | 0.524 |
| 2 | **Resource Retirement (before + after publish)** | P1 | 4.282 | **4.432** | 4.368 |
| — | └ Before publish (`lock_hold = 2.325`, `release = 0.742`, `finish = 0.516`, `remove = 0.342`, `collect = 0.084`) | P1 | 2.406 | 2.525 | 2.417 |
| — | └ After publish (`lock_hold = 1.545`, `remove = 0.484`, `finish = 0.185`, `collect = 0.122`) | P1 | 1.876 | 1.907 | 1.951 |
| 3 | **Coalescing (`coalescing_cpu_ns`)** | P1 | 2.030 | **2.501** | 2.422 |
| — | └ Progress wait (`coalescing_progress_wait_cpu_ns`) | — | 0.717 | 1.361 | — |
| — | └ Active coalescing (`lock_hold = 0.710`, `digest_fold = 0.201`, `completion_apply = 0.098`, `completion_drain = 0.091`, `cursor_advance = 0.062`) | P1 | 0.840 | 1.140 | 1.041 |
| 4 | **FRONTIER Write (`frontier_write_cpu_ns`)** | P1 | 0.796 | **1.095** | 1.029 |
| 5 | **FRONTIER Preparation (`preparation_cpu_ns`)** | P1 | 0.713 | **0.960** | 0.884 |
| 6 | **Coordinator Publish (`coordinator_publish_cpu_ns`)** | P1 | 0.600 | **0.611** | 0.591 |
| 7 | **fdatasync (`fdatasync_cpu_ns`)** | — | 0.200 | **0.408** | 0.428 |

## 3. Post-WAL / MVCC & Digest Signals

- **ACK / Post-WAL Wall-Clock Latencies (`1,562` sampled Puts, wall time, not CPU)**:
  - `durable_publish_to_wal_return_ns`: `31.185 μs` median / `34.688 μs` mean.
  - `wal_return_to_put_return_ns`: `12.806 μs` median / `18.953 μs` mean.
  - `mvcc_publish_commit_ts_ns`: `6.500 μs` median / `13.179 μs` mean (P2: wall-clock tail vs median `6.5 → 13.2 μs` indicates MVCC commit-ts publication contention/work).
  - `memtable_publish_wall_ns`: `2.558 μs` median / `3.338 μs` mean.
- **V8 Digest CPU (`1,563` sampled tickets at `1:64`)**:
  - `payload_hash_cpu_ns`: `2.785 μs/put` (`4,353,619 ns / 1,563`).
  - `header_batch_hash_cpu_ns`: `0.542 μs/put` (`847,189 ns / 1,563`).

## 4. Takeaways for Next Optimization PRs

1. **Within the Durability Driver (`10.95 μs/put`)**, the largest single CPU bucket is **Resource + Guard Retirement (`4.432 μs/put`, 40.5% of driver CPU)**, where lock hold alone is `2.325 + 1.545 = 3.870 μs/put`. Completion drain (`0.091 μs/put`), completion apply (`0.098 μs/put`), and cursor advance (`0.062 μs/put`) are already negligible (`0.25 μs/put` combined).
2. **Follower Wait (`9.790 μs/put`)** improved by `2.12 μs/put` after PR #571, with `coordinator_lock_wait_cpu` at `0.652 μs/put`; the remaining `~9.1 μs/put` is thread park/wake/condvar overhead.
3. **~52.6 μs/put (71.7% of total CPU)** lies outside `driver_cpu` and `follower_wait_cpu` (Producer admission/encoding/submission, io_uring DATA worker/reaper, MVCC commit, SkipList insert). A producer/worker CPU attribution run (`--wal-cpu-attribution-sample-every`) is needed next if we target that `52.6 μs/put` bucket.
