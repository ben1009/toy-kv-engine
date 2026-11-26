# Issue #570 Full Point-Write CPU Attribution (Point1 vs. Point16 on `main`)

- **Date**: 2026-11-26
- **Issue**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570)
- **Commit**: `88845778` (`main`, post-PR #571/#572, Candidate Target8 default)
- **Environment**: `c4-standard-16-lssd` (`us-central1-b`), 2× NVMe Local SSD RAID0 (`/mnt/test-ssd`, ext4), `write-perf --bench wal_concurrent --wal --pitr --pitr-format v8 --wal-io-mode parallel --num 100000 --value-size 1024 --compaction none --wal-cpu-attribution-sample-every 64 --wal-v8-durability-sample-every 64` (3 runs each at 1 writer and 16 writers)
- **Local archive**: `bench-results/gcp-issue570-cpu-attr-20261126/issue570-cpu-attr.tar.gz` (`sha256: 9bcf30b0896d5e0fca5a20c2efe20b7e97b68a130b00514acae4a77a98c191d2`)

## 1. Per-Run Closed CPU Budget & Sample Completeness (μs/Put)

Every individual run's `Total Process CPU/Put` equals `Producer + Driver + Follower + io_worker_completion + Unattributed Remainder`. All 6 runs recorded `operations_seen = 100,000`, `selected_producer_samples = 1,562`, and `0` dropped samples across producer (`dropped_producer_samples = 0`), background thread (`dropped_thread_samples = 0`), durability advance (`dropped_samples = 0`), and follower wait (`follower_wait_dropped_samples = 0`).

| Run | Throughput | Total CPU | Producer | Driver | Follower | `io_worker` Compl. | Unattributed Remainder | Durability Samples |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `point1-r1` | 8,632.5 puts/s | 51.78 | 12.80 | 20.53 | 0.00 | 3.34 | 15.12 | 1,562 |
| `point1-r2` | 8,598.4 puts/s | 53.29 | 12.68 | 20.72 | 0.00 | 3.49 | 16.40 | 1,562 |
| `point1-r3` | 8,550.8 puts/s | 53.99 | 12.33 | 20.55 | 0.00 | 3.60 | 17.52 | 1,562 |
| `point16-r1` | 45,379.8 puts/s | 75.25 | 29.89 | 11.03 | 11.80 | 1.69 | 20.85 | 187 |
| `point16-r2` | 46,421.6 puts/s | 76.52 | 29.88 | 11.18 | 12.27 | 1.64 | 21.56 | 187 |
| `point16-r3` | 49,055.4 puts/s | 74.16 | 29.77 | 11.15 | 11.83 | 1.67 | 19.73 | 187 |

Summary below reports **per-column medians** across the 3 runs (independent column medians do not sum identically to the total median):

| Component | Point1 Median (1t) | Point16 Median (16t) | Delta (P16 − P1) | Accounting |
| --- | ---: | ---: | ---: | --- |
| **Total Process CPU/Put** | **53.29** | **75.25** | **+21.96** | `(user_ms + system_ms) * 1000 / 100,000` |
| **Producer WAL Path (`producer_total_cpu_ns`)** | **12.68** | **29.88** | **+17.20** | Mean of `1:64` producer samples |
| **Durability Driver (`driver_cpu_ns`)** | **20.55** | **11.15** | **−9.40** | `mean(driver_cpu_ns) * completed_advances / (1000 * puts)` |
| **Follower Entry Lock + Wait** | **0.00** | **11.82** | **+11.82** | `total_wait_cpu` (`11.02`) + pre-wait `coordinator_lock_wait_cpu` (`0.79`) via `sum * 64 / puts` |
| **`io_worker_completion_processing`** | **3.49** | **1.67** | **−1.82** | `sum(thread_cpu_ns) * 64 / (1000 * puts)` (`1.00` → `2.22` items/event) |
| **Unattributed Remainder** | **16.40** | **20.85** | **+4.45** | `Total − (Producer + Driver + Follower + io_worker_completion)`; includes profiling/sampling overhead and unmeasured stages (e.g. MemTable, MVCC pub, `io_worker` submit, `Put` wrapper) |

- **Point1** median throughput: `8,598.4 puts/s` (`100,000` syncs, `1.00` put/sync).
- **Point16** median throughput: `46,421.6 puts/s` (`12,011` syncs, `8.33` puts/sync; diagnostic build overhead vs `49,642` puts/s / `70.92 μs/put` unprofiled).

## 2. Producer WAL Sub-Phase Breakdown (Median of Run Means, μs/Put)

Inclusive vs. exclusive relationships are preserved from `WalCpuAttributionObservation`:

| Producer Sub-phase | Accounting | Point1 (μs/put) | Point16 (μs/put) | Delta (P16 − P1) |
| --- | --- | ---: | ---: | ---: |
| **`producer_total_cpu_ns`** | Inclusive total | **12.678** | **29.876** | **+17.198** |
| **`sequencer_wait_cpu_ns`** | Exclusive (before sequencer hold) | 0.457 | **4.996** | **+4.539** (`38.44 μs` wall) |
| **`sequencer_hold_cpu_ns`** | Inclusive (sequencer lock hold) | 2.707 | **6.897** | **+4.190** |
| └ `commit_setup_cpu_ns` | Inside sequencer hold | 0.326 | 0.828 | +0.502 |
| └ `admission_cpu_ns` | Inside sequencer hold | 1.473 | **4.140** | **+2.667** |
| **`preflight_cpu_ns`** | Exclusive (before sequencer) | 0.313 | **2.198** | **+1.885** |
| **`point_buffer_preallocation_cpu_ns`** | Exclusive (Stage A prealloc) | 0.322 | **1.317** | **+0.995** |
| **`input_build_cpu_ns`** | Exclusive | 0.443 | 0.654 | +0.211 |
| **`frame_finish_cpu_ns`** | Inclusive (encode + digest + submit) | 7.170 | **10.769** | **+3.599** |
| └ `v8_logical_encode_cpu_ns` | Inside `frame_finish` | 1.768 | **3.104** | **+1.336** |
| └ `v8_digest_ready_cpu_ns` | Inside `frame_finish` | 0.240 | 0.386 | +0.146 |
| └ `v8_frame_encode_cpu_ns` | Inside `frame_finish` | 0.540 | 0.667 | +0.127 |
| └ `v8_frames_ready_submit_cpu_ns` | Inside `frame_finish` | 3.238 | **4.377** | **+1.139** |

## 3. Observations & Next Optimization Target

1. **Producer WAL Path (`12.68 → 29.88 μs/put`, `+17.20 μs/put`)** is the largest measured source of Point16 CPU growth:
   - **Sequencer Wait + Hold (`0.457 + 2.707 = 3.164` → `4.996 + 6.897 = 11.893 μs/put`, `+8.729 μs/put`)**: `sequencer_wait_cpu_ns` directly measures sequencer lock waiting (`4.996 μs/put` CPU, `38.44 μs` wall), and `admission_cpu_ns` inside the sequencer hold grows from `1.473` to `4.140 μs/put`.
   - **`preflight_cpu_ns` (`0.313 → 2.198 μs/put`)**, **`point_buffer_preallocation_cpu_ns` (`0.322 → 1.317 μs/put`)**, and **`frame_finish_cpu_ns` (`7.170 → 10.769 μs/put`)** also increase under 16-writer concurrency (observed CPU growth; specific hardware cache vs. inner-lock causes are not separately instrumented here).
2. **Follower Wait (`0.00 → 11.82 μs/put`)** is largely offset by **Durability Driver amortization (`20.55 → 11.15 μs/put`, `-9.40 μs/put`)**.
3. **Unattributed Remainder (`16.40 → 20.85 μs/put`, `+4.45 μs/put`)** includes profiling/sampling overhead and any unmeasured write-path stages.
4. **Next step**: Audit `admit_prepared_v8_point_pitr_wal_batch_only()` and the MVCC Sequencer critical section (`11.893 μs/put`) to move work that does not depend on Commit TS / WAL Ticket order outside the Sequencer lock.
