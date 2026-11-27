# Issue #570 Full CPU Attribution & `perf` Flamegraph Diagnostic (`a599fd33`)

- **Date**: 2026-11-27
- **Issue / Baseline**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / `a599fd33a2c4a55f2a2a0b4a9a9b9f17a26a0c6d` (`main`, post-#580)
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/gcp-wal-bench`, `ext4`), `write-perf --bench wal_concurrent --wal --pitr --pitr-format v8 --wal-io-mode parallel --num 100000 --value-size 1024 --compaction none --latency-sample-every 100` (`TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`).
- **Local archive**: `bench-results/gcp-cpu-attr-a599fd33` (`cpu-attr-a599fd33.tar.gz` sha256: `0c4c5df20e9867aa0b7ca5b44b814bd3558c04f81aa9b647e4a0d578b69502c9`, 6 attribution JSONs + 2 `perf` DWARF captures).

> [!NOTE]
> Diagnostic runs only (`--wal-cpu-attribution-sample-every 64 --wal-v8-durability-sample-every 64 --mutex-lock-sample-every 64 --mvcc-publication-sample-every 64`, plus separate `perf record -F 999 -g --call-graph dwarf,16384` captures); not scored as unprofiled throughput.

---

## 1. Executive Summary: Primary Bottleneck Found

`perf` DWARF profiling (`5,952` samples on **Point16**) and mutex lock diagnostics reveal that **the single largest CPU bottleneck in Point16 is no longer WAL submission or durability—it is MVCC timestamp publication (`LsmMvccInner::publish_commit_ts_impl`)**:

1. **`LsmMvccInner::publish_commit_ts_impl` accounts for 19.88% of self-CPU (23.44% inclusive) in Point16** (~13–16 μs/Put out of 68.77 μs/Put instrumented / 64.56 μs/Put unprofiled).
2. **Root cause in [`mvcc.rs`](../../../kv-engine/src/mvcc.rs#L27-L30)**:
   - `PUBLICATION_SPIN_LIMIT = 1 << 12` (**4,096 spin iterations**), while `PUBLICATION_CONTENTION_THRESHOLD = 32` and `PUBLICATION_DISTANCE_THRESHOLD = 16`.
   - Under **Point16** (16 writers), `in_flight <= 16`, so `in_flight > 32` and `in_flight > 16` are **never true**. Every out-of-order writer spins up to **4,096 times** on `self.frontier.load(Ordering::Acquire)` + `spin_loop()` before falling back to `Condvar::wait`.
   - In addition, after `await_publication` returns, `publish_commit_ts_impl` **re-acquires the `mvcc_publication` mutex a second time** (`169,888` acquisitions for `100,000` ops, **42.1% contended**, `~2.40 μs/Put` lock wait).
3. **Second largest non-WAL / coordination cluster**: `parking_lot` mutex/condvar slow paths and kernel futex/schedule transitions account for **~19.15%** of Point16 leaf samples (shared across `mvcc_publication`, `reservation_owner`, `v8_extent_state`, and `WalV8ParallelCoordinator` follower/driver wakeups).

---

## 2. In-App CPU Attribution Breakdown (Median of 3 Runs Each)

### 2.1 Overall & Producer Phases (ns/Put)

| Metric | Point1 (1t) | Point16 (16t) | Notes |
| --- | ---: | ---: | --- |
| Throughput (instrumented) | 8,887 ops/s | 52,780 ops/s | — |
| Total Process CPU / Put | 52.49 μs (27.79u + 24.46s) | 68.77 μs (45.90u + 22.76s) | — |
| `input_build_cpu_ns` | 375.0 ns | 484.5 ns | — |
| `preflight_cpu_ns` | 273.0 ns | 417.5 ns | — |
| `point_buffer_preallocation_cpu_ns` | 294.0 ns | 499.0 ns | `try_preallocate_point_buffer` |
| `sequencer_wait_cpu_ns` | 425.0 ns | 2,823.5 ns | Sequencer lock wait |
| `sequencer_hold_cpu_ns` | 2,027.0 ns | 3,945.0 ns | Sequencer critical section |
| `commit_setup_cpu_ns` | 274.0 ns | 456.0 ns | — |
| `admission_cpu_ns` | 886.0 ns | 2,070.5 ns | — |
| `frame_finish_cpu_ns` | 6,594.0 ns | 7,718.5 ns | Includes logical encode + submit |
| `v8_logical_encode_cpu_ns` | 1,641.0 ns | 2,056.5 ns | BLAKE3 + frame encode |
| `v8_frames_ready_submit_cpu_ns` | 3,278.5 ns | 2,756.0 ns | `publish_data` + `submit_group` |
| **`producer_total_cpu_ns`** | **11,375.5 ns** | **21,392.0 ns** | **31.1% of Point16 total CPU** |

### 2.2 Durability Driver, Follower & I/O Worker

| Component | Point1 (1t) | Point16 (16t) | Notes |
| --- | ---: | ---: | --- |
| `tickets_covered` / advance | 1.0 | 8.0 | — |
| `driver_cpu_ns` / advance | 18,337.0 ns (18.34 μs/put) | 75,048.0 ns (9.38 μs/put) | — |
| `coalescing_cpu_ns` / advance | 0.0 ns | 15,764.0 ns (1.97 μs/put) | `active=6,403.5 ns`, `progress_wait=3,039.0 ns` |
| `preparation_cpu_ns` / advance | 3,937.5 ns | 5,256.5 ns | — |
| `frontier_write_cpu_ns` / advance | 4,927.0 ns | 6,632.0 ns | `pwrite` FRONTIER frame |
| `fdatasync_cpu_ns` / advance | 1,050.0 ns | 1,639.0 ns | — |
| `res_retire` before + after publish | 4,190.5 ns | 23,889.5 ns (2.99 μs/put) | `before=12,646.5`, `after=11,243.0` |
| `coordinator_publish_cpu_ns` | 917.0 ns | 4,889.0 ns | — |
| `follower.total_wait_cpu_ns` | — | 10,338.0 ns | `condvar_wait_count=2.0`, `notify_to_resume=17.1 μs` |
| `io_worker_completion_processing` | 3,315 ns/group | 2,930 ns/group | `wal-io-worker` thread |

### 2.3 Mutex Lock Diagnostics (100k ops)

| Lock Name | Point1 (`acq` · `contended` · `wait_total`) | Point16 (`acq` · `contended` · `wait_total`) |
| --- | --- | --- |
| `mvcc_publication` | 100,000 · 0 (0.0%) · 2.87 ms | **169,888 · 71,562 (42.1%) · 240.42 ms (2.40 μs/put)** |
| `v8_extent_state` | 825,157 · 128 (0.02%) · 49.43 ms | 534,847 · 24,282 (4.5%) · 75.60 ms (0.76 μs/put) |
| `reservation_owner` | 400,000 · 0 (0.0%) · 20.68 ms | 310,746 · 15,518 (5.0%) · 36.65 ms (0.37 μs/put) |
| `resource_ledger` | 400,000 · 0 (0.0%) · 20.21 ms | 310,746 · 0 (0.0%) · 25.70 ms (0.26 μs/put) |
| `v8_extent_preparation` | 197,506 · 0 (0.0%) · 19.61 ms | 97,349 · 128 (0.1%) · 8.35 ms |

---

## 3. `perf` DWARF CPU Flamegraph Hotspots (`perf record -F 999 -g --call-graph dwarf,16384`)

### 3.1 Point16 (`5,952` samples) Top Self-CPU Functions

| Self % | Samples | Symbol / Component | Category |
| ---: | ---: | --- | --- |
| **19.88%** | **1,183** | `LsmMvccInner::publish_commit_ts_impl` (`await_publication` spin-loop) | **Non-WAL: MVCC** |
| **4.35% + 1.04%** | 321 | `[k] finish_task_switch` (`write-perf` + `wal-io-worker`) | Kernel Sched / Futex |
| **3.58% + 0.52% + 0.47%** | 271 | `[k] _raw_spin_unlock_irqrestore` / `_raw_spin_unlock_irq` | Kernel Sched / Futex |
| **3.07%** | 183 | `parking_lot::raw_mutex::RawMutex::lock_slow` | Lock Contention |
| **2.28%** | 136 | `_blake3_compress_in_place_avx512` | WAL Frame Encoding |
| **2.17% + 0.60%** | 165 | `[k] __schedule` / `schedule` | Kernel Sched / Futex |
| **1.73% + 1.24%** | 177 | `libc syscall` + `[k] do_syscall_64` | Kernel Syscall |
| **1.21% + 1.21% + 1.11% + 0.79% + 0.69% + 0.49%** | 327 | `[k] futex_*` (`__futex_wait`, `futex_unqueue`, `futex_wake`, etc.) | Kernel Futex |
| **1.16%** | 69 | `libc malloc` | Allocator (`2.37%` total leaf alloc) |
| **1.08%** | 64 | `MemTable::publish_raw_batch` | Non-WAL: MemTable |
| **1.02% + 1.01%** | 121 | `Condvar::wait_until_internal` + `RawMutex::unlock_slow` | Lock / Condvar |
| **0.96%** | 57 | `LsmStorageInner::put_with_lifecycle_admission` | LSM Dispatch |
| **0.91% + 0.91%** | 108 | `finish_with_point_direct_frame_and_phases` + `crc32fast` | WAL Frame Encoding |
| **0.82%** | 49 | `crossbeam_skiplist::SkipList::search_position` | Non-WAL: MemTable |

### 3.2 Point1 (`4,529` samples) Top Self-CPU Functions

| Self % | Samples | Symbol / Component | Category |
| ---: | ---: | --- | --- |
| **4.24%** | 192 | `sha2::sha256::x86::digest_blocks` | WAL / PITR Digest |
| **4.15% + 0.82%** | 225 | `crossbeam_channel::waker::SyncWaker::notify` | I/O Worker Handoff |
| **3.71%** | 168 | `_blake3_compress_in_place_avx512` | WAL Frame Encoding |
| **2.89% + 2.27%** | 234 | `[k] _raw_spin_unlock_irqrestore` (`write-perf` + `wal-io-worker`) | Kernel Futex/Sched |
| **2.58% + 2.27%** | 220 | `crossbeam_channel` `recv` (`wal-io-worker` + `write-perf`) | I/O Worker Handoff |
| **1.44%** | 65 | `WalV8ParallelWriter::drive_frontier_through` | Durability Driver |

### 3.3 Cross-Cutting Overhead Summary (`perf` Leaf Samples)

| Bucket | Point1 | Point16 |
| --- | ---: | ---: |
| **MVCC `publish_commit_ts_impl` (spin + lock)** | 0.88% | **23.44% (~15 μs/Put)** |
| **Kernel total (futex, sched, syscalls, io_uring)** | 46.26% | 33.57% |
| **Futex / Schedule / `parking_lot` leaf** | 19.89% | 19.15% |
| **`wal-io-worker` thread total** | 35.92% | 13.00% |
| **Allocator (`malloc`/`free`) leaf** | 2.38% | 2.37% (~1.5 μs/Put) |

---

## 4. Recommended Next Optimization Directions (Target: 2–3+ μs/Put)

1. **P0 — Fix MVCC `publish_commit_ts_impl` spin & double-lock (Est. potential: 8–12 μs/Put)**:
   - Under `Point16`, `in_flight <= 16` never triggers `CONTENDED_PUBLICATION_SPIN_LIMIT`, causing every out-of-order publisher to spin **4,096 iterations** (`19.88%` of total CPU!) and then re-acquire `mvcc_publication` mutex a second time (`42.1%` contended, `2.40 μs/Put`).
   - Direction: eliminate the 4,096-iteration blind spin under multi-writer contention (or use a small bounded spin e.g. 0–64 when `in_flight > 1` / `commit_ts != next_to_publish`), and avoid re-acquiring `self.publication` after `await_publication` returns (decrement `in_flight` atomically / check poison without a second mutex round-trip).
2. **P1 — Reduce `parking_lot` / Futex wakeup churn across Driver ↔ Follower & `mvcc_publication` (Est. potential: 2–3 μs/Put)**:
   - `Condvar::notify_all` / `lock_slow` + kernel futex/schedule accounts for ~19% of leaf samples; targeted/single-waiter wakeups or batching publication advances can cut futex wakeups.

---

## 5. Cleanup Verification

- Verified local archive checksum (`0c4c5df20e9867aa0b7ca5b44b814bd3558c04f81aa9b647e4a0d578b69502c9`), all 6 attribution JSONs, and both `perf` captures before teardown.
- Deleted `walbench-diag-vm`, `walbench-diag-ssh`, and destroyed `walbench-isolated-vpc`, `walbench-isolated-subnet`, and `walbench-isolated-egress`.
