# Issue #570 Full Point-Write CPU Attribution (Point1 vs. Point16 on `main`)

- **Date**: 2026-11-26
- **Issue**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570)
- **Commit**: `88845778` (`main`, post-PR #571/#572, Candidate Target8 default)
- **Environment**: `c4-standard-16-lssd` (`us-central1-b`), 2× NVMe Local SSD RAID0 (`/mnt/test-ssd`, ext4), `write-perf --bench wal_concurrent --wal --pitr --pitr-format v8 --wal-io-mode parallel --num 100000 --value-size 1024 --compaction none --wal-cpu-attribution-sample-every 64 --wal-v8-durability-sample-every 64` (3 runs each at 1 writer and 16 writers)
- **Local archive**: `bench-results/gcp-issue570-cpu-attr-20261126/issue570-cpu-attr.tar.gz` (`sha256: 9bcf30b0896d5e0fca5a20c2efe20b7e97b68a130b00514acae4a77a98c191d2`)

## 1. Closed Total CPU Budget (Median of 3 Runs, μs/Put)

| Component | Point1 (1t) | Point16 (16t) | Delta (P16 − P1) | Accounting |
| --- | ---: | ---: | ---: | --- |
| **Total Process CPU/Put** | **53.29** | **75.25** | **+21.96** | `(user_ms + system_ms) * 1000 / 100,000` |
| **Producer WAL Path (`producer_total_cpu_ns`)** | **12.68** | **29.88** | **+17.20** | Mean of `1:64` producer samples |
| **Durability Driver (`driver_cpu_ns`)** | **20.55** | **11.15** | **−9.40** | `mean(driver_cpu_ns) * completed_advances / (1000 * puts)` |
| **Follower Entry Lock + Wait** | **0.00** | **11.82** | **+11.82** | `total_wait_cpu` (`11.02`) + pre-wait `coordinator_lock_wait_cpu` (`0.79`) via `sum * 64 / puts` |
| **`io_worker_completion_processing`** | **3.49** | **1.67** | **−1.82** | `sum(thread_cpu_ns) * 64 / (1000 * puts)` (`1.00` → `2.22` items/event) |
| **Remaining (Non-Attributed Path)** | **16.40** | **20.85** | **+4.45** | `Total − (Producer + Driver + Follower + io_worker_completion)` (MemTable SkipList, MVCC pub, `io_worker` SQE submit, `Put` wrapper) |

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

## 3. Where the `+21.96 μs/put` Point1 → Point16 Gap Comes From

1. **Producer WAL Path (`12.68 → 29.88 μs/put`, `+17.20 μs/put`)** is the single largest source of Point16 CPU overhead:
   - **Sequencer & Admission contention (`sequencer_wait` + `admission` + `preflight` + `point_buffer_preallocation`)** adds `+10.09 μs/put` (`4.54 + 2.67 + 1.89 + 1.00`).
   - **Frame Finish (`frame_finish_cpu_ns`: `7.17 → 10.77 μs/put`, `+3.60 μs/put`)**: `v8_logical_encode` (`+1.34 μs/put`) and `v8_frames_ready_submit` (`+1.14 μs/put`) grow under 16-thread cache/mutex contention.
2. **Follower Wait (`0.00 → 11.82 μs/put`)** adds `+11.82 μs/put`, largely offset by **Durability Driver amortization (`20.55 → 11.15 μs/put`, `-9.40 μs/put`)**.
3. **Remaining Non-Attributed Path (`16.40 → 20.85 μs/put`, `+4.45 μs/put`)** covers MemTable SkipList insert, MVCC commit-ts publication, `io_worker` SQE submission loop, and `Put` wrapper overhead.
4. **`io_worker_completion_processing` (`1.67 μs/put` at Point16)** is minor.
