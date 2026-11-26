# Issue #570 Point16 CPU Attribution Breakdown (From Retained GCP Archive)

- **Date**: 2026-11-26
- **Issue**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570)
- **Source Archive**: `bench-results/gcp-issue570-ab-20261126/issue570-ab/diag-point16-{baseline,candidate}/result.json` (`sha256` of `issue570-ab.tar.gz`: `a17e4e2bbbe0eed46fafd23a3c548f328892ef08e896eef51602fc22f16b6939`)
- **Environment**: `c4-standard-16-lssd` (`us-central1-b`), 16 writers, 100,000 puts, 1 KiB values, Candidate Target8 default (`30c9092d`), `--wal-v8-durability-sample-every 64` (no new GCP run; extracted from retained archive)

## 1. Sampling Basis & Normalization

- **Driver Advance samples** (`next_wal_v8_durability_advance_sample_candidate`, 1 in 64 completed advances):
  - Baseline (`20d83e81`): `186` sampled advances (`dropped_samples = 0`) out of `11,943` completed advances; `sum(tickets_covered) = 1,602` (mean `8.613` tickets/advance).
  - Candidate (`30c9092d`): `186` sampled advances (`dropped_samples = 0`) out of `11,961` completed advances; `sum(tickets_covered) = 1,556` (mean `8.366` tickets/advance).
  - Normalized to `μs/put`: `mean(sample_cpu_ns) * completed_advances / (1000 * total_puts)` (cross-checked against `sum(sample_cpu_ns) * 64 / (1000 * total_puts)`).
- **Follower Wait samples** (`next_wal_v8_follower_sample`, 1 in 64 callers entering the follower wait path):
  - Baseline: `1,549` samples (`dropped = 0`, `sample_id` `64..99,136`), ~`0.991` follower-wait entries/put.
  - Candidate: `1,534` samples (`dropped = 0`, `sample_id` `64..98,176`), ~`0.982` follower-wait entries/put.
  - Normalized to `μs/put`: `sum(sample_cpu_ns) * 64 / (1000 * total_puts)`.
  - Note: `coordinator_lock_wait_cpu_ns` measures the **initial** `state.lock()` acquisition upon entering `acquire_durability_driver()` **before** `total_wait_cpu_ns` starts timing; the two are additive, not overlapping.

## 2. High-Level Point16 CPU Budget

- **Unprofiled Point16 median CPU/Put** (40-run A/B): `74.75 μs` (baseline `20d83e81`) → **`70.92 μs`** (candidate `30c9092d`, `-5.12%` by arm medians; `-5.78%` paired median across the 5 counterbalanced pairs).
- **Diagnostic Point16 total CPU/Put** (`--wal-v8-durability-sample-every 64`): `79.23 μs` (baseline, `11,943` syncs) → **`73.33 μs`** (candidate, `11,961` syncs).
- **Target**: `<= 60.00 μs/put` (`~10.9 μs/put` remaining gap from `70.92 μs`).

| Coarse Bucket (Diagnostic Run) | Baseline Mean (μs/put) | Candidate Mean (μs/put) | Share of 73.33 μs | Scope |
| --- | ---: | ---: | ---: | --- |
| **Driver Advance (`driver_cpu_ns`)** | 11.180 (`11.143` ×64) | **10.959** (`10.907` ×64) | 14.9% | Inclusive driver loop (`91.62 μs/advance` candidate vs `93.61 μs/advance` baseline) |
| **Follower Entry Lock + Wait** | 14.001 | **11.645** | 15.9% | Additive: `total_wait_cpu_ns` (`10.917`) + pre-wait `coordinator_lock_wait_cpu_ns` (`0.728`) |
| **Other / Unattributed Write Path** | ~54.09 | **~50.77** | 69.2% | Producer admission/encode/submit, io_uring DATA worker/reaper, MVCC, SkipList |

## 3. Detailed Driver & Follower Breakdown (μs/put, Mean)

Inclusive vs. exclusive relationships are marked explicitly so sub-phases are not double-counted:

| Stage / Sub-phase | Accounting | Baseline (μs/put) | Candidate (μs/put) | Notes |
| --- | --- | ---: | ---: | --- |
| **Follower `total_wait_cpu_ns`** | Inclusive (wait loop) | 13.411 | **10.917** | `-2.494 μs/put` (`-18.6%`); includes condvar wait, wake, and re-lock attempts |
| **Follower `coordinator_lock_wait_cpu_ns`** | Exclusive (pre-wait) | 0.590 | **0.728** | Initial `state.lock()` before `total_wait_cpu_ns` begins |
| **Resource Retire Before Publish** | Inclusive | 2.487 | **2.526** | `lock_hold` (`2.326` cand.) includes `collect` (`0.084`), `remove` (`0.343`), `release` (`0.742`), `finish` (`0.516`) |
| **Resource Retire After Publish** | Inclusive | 2.007 | **1.909** | `lock_hold` (`1.546` cand.) includes `collect` (`0.122`), `remove` (`0.485`), `finish` (`0.185`) |
| **Coalescing (`coalescing_cpu_ns`)** | Inclusive | 2.492 | **2.502** | `progress_wait` (`1.362`) + `active` (`1.140`); `lock_hold` (`0.711`) includes drain (`0.091`), apply (`0.099`), digest fold (`0.201`), cursor advance (`0.062`) |
| **FRONTIER Write (`frontier_write_cpu_ns`)** | Exclusive | 1.059 | **1.095** | FRONTIER I/O submission/completion |
| **FRONTIER Prep (`preparation_cpu_ns`)** | Exclusive | 0.910 | **0.961** | Extent check, reservation, frame encode + digest |
| **Coordinator Publish (`coordinator_publish_cpu_ns`)** | Exclusive | 0.608 | **0.611** | `publish_durable_frontier_sampled()` |
| **fdatasync (`fdatasync_cpu_ns`)** | Exclusive | 0.440 | **0.408** | Syscall CPU only |
| **Unattributed Driver Remainder** | Residual | 1.187 | **0.947** | `driver_cpu_ns` minus measured driver sub-phases (`advance_and_snapshot_after_wake`, `frontier.lock()`, `mark_durable`, loop/sampling overhead) |

## 4. Post-WAL / MVCC & Digest Signals

- **ACK / Post-WAL Wall-Clock Latencies (`1,562` sampled Puts, wall time, not thread CPU)**:
  - `durable_publish_to_wal_return_ns`: `31.185 μs` median / `34.688 μs` mean.
  - `wal_return_to_put_return_ns`: `12.806 μs` median / `18.953 μs` mean.
  - `mvcc_publish_commit_ts_ns`: `6.500 μs` median / `13.179 μs` mean.
  - `memtable_publish_wall_ns`: `2.558 μs` median / `3.338 μs` mean.
- **V8 Digest CPU (`1,563` sampled tickets at `1:64`)**:
  - `payload_hash_cpu_ns`: `2.785 μs/put` (`4,353,619 ns / 1,563`).
  - `header_batch_hash_cpu_ns`: `0.542 μs/put` (`847,189 ns / 1,563`).

## 5. Next Step: Full Point-Write CPU Attribution

- **Durability Driver (`10.959 μs/put`)** and **Follower Wait (`11.645 μs/put`)** together account for **`~22.6 μs/put` (30.8%)** of total CPU. Even eliminating `resource_retire_*` (`4.435 μs/put` combined inclusive) cannot close the `~10.9 μs/put` gap to `<= 60 μs/put`.
- **`~50.77 μs/put` (69.2% of total CPU)** sits in the remaining Point write path. Next, collect `--wal-cpu-attribution-sample-every` across both **Point1** (fixed single-Put cost) and **Point16** (concurrent coordination cost) covering:
  - **P0**: Producer Admission / MVCC Sequencer, Frame Preparation / Encoding, DATA Submission / `io_uring` Worker
  - **P1**: MVCC Publication, MemTable / SkipList Insert
  - **P2**: Resource / Guard Retirement
