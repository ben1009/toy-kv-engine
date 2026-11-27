# Issue #570 MVCC Publication Spin Budget Experiment (`4096` vs `256` vs `64`)

- **Date**: 2026-11-27
- **Issue / Baseline**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / `ea65d941` (`main`, code identical to `a599fd33a298f1ee10dd6f5de81ec8f1a889d213`)
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/gcp-wal-bench`, `ext4`), `write-perf --wal --pitr --pitr-format v8 --wal-io-mode parallel --num 100000 --value-size 1024 --compaction none --latency-sample-every 100` (`TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`).
- **Arms tested**:
  - **Baseline (`4096`)**: `PUBLICATION_SPIN_LIMIT = 1 << 12` (`write-perf-4096`, sha256 `752bf27a43592f8b938c26283403f12e43ad50b6c0cf3e805412e814397fd99d`)
  - **Candidate A (`256`)**: `PUBLICATION_SPIN_LIMIT = 256` (`write-perf-256`, sha256 `f88f7df0bad42fe12cbc461f22148535a2d6bcfb70e8651b9c9139e60999ee63`)
  - **Candidate B (`64`)**: `PUBLICATION_SPIN_LIMIT = 64` (`write-perf-64`, sha256 `485bf156902e69d9ccd51c2ed0831291534b010e03c735789e8271b6ee425bed`)
- **Local archive**: `bench-results/gcp-mvcc-spin-20261127/mvcc-spin.tar.gz` (`sha256: ed99bd59dc8089faf731ef6e9c54eca058157ac118303e430a78571690f4a941`, 48 unprofiled A/B/C runs + 3 diagnostic runs verified).

---

## 1. Counterbalanced A/B/C Results (Unprofiled)

| Workload | `4096` (Baseline) | `256` (Candidate A) | `64` (Candidate B) |
| --- | ---: | ---: | ---: |
| **Point16** (5 triples) | 53,483/s · 65.41 μs · 1.364 ms p99 | 51,967/s (**-2.60%**, 0/5) · **62.46 μs (-4.64%, 5/5)** · **1.009 ms (-23.91%)** | 52,212/s (**-2.29%**, 0/5) · **61.04 μs (-6.74%, 5/5)** · 1.095 ms (+17.31%) |
| **Point8** (5 triples) | 36,901/s · 43.63 μs · 0.851 ms p99 | 36,788/s (-0.31%, 2/5) · **41.28 μs (-5.20%, 5/5)** · 0.824 ms (-3.82%) | 36,171/s (-1.37%, 2/5) · **41.62 μs (-4.42%, 5/5)** · 0.807 ms (-5.34%) |
| **Point1** (3 triples) | 8,643/s · 52.52 μs · 0.476 ms p99 | 8,666/s (+1.57%, 2/3) · 52.09 μs (-0.94%, 2/3) · 0.479 ms (-12.42%) | 8,436/s (-1.12%, 0/3) · 52.17 μs (-0.65%, 2/3) · 0.663 ms (+39.18%) |
| **Batch16** (3 triples) | 411,879/s · 7.79 μs · 1.826 ms p99 | 409,005/s (-0.70%, 1/3) · **7.69 μs (-2.21%, 3/3)** · 1.486 ms (-18.63%) | **415,115/s (+1.47%, 3/3)** · **7.63 μs (-2.25%, 3/3)** · 1.083 ms (+4.24%) |

---

## 2. Diagnostic Mechanism (`--mvcc-publication-sample-every 64 --wal-cpu-attribution-sample-every 64`, Point16)

> [!NOTE]
> Diagnostic runs only (`bench-results/gcp-mvcc-spin-20261127/mvcc-spin-diag`); not scored as unprofiled throughput.

| Spin Limit | `ops/s` (diag) | `CPU/put` (diag) | `exits` (`not_req` / `frontier_adv` / `exhausted`) | `spin_iter` (all mean / waited med) | `condvar_wait_calls` (mean) | `mvcc_cpu` (all mean / waited mean) |
| ---: | ---: | ---: | --- | ---: | ---: | ---: |
| **`4096`** | 53,404/s | 64.92 μs | 583 / 928 / **51 (3.3%)** | 808.4 / 987 | **0.033** | 14,146 ns / 21,050 ns |
| **`256`** | 52,457/s | 62.28 μs | 588 / 148 / **826 (52.9%)** | 145.0 / 256 | **0.597** (18×) | 12,503 ns / 17,519 ns |
| **`64`** | 51,880/s | 61.63 μs | 572 / 38 / **952 (60.9%)** | 39.8 / 64 | **0.695** (21×) | 11,995 ns / 16,336 ns |

- **Key insight**:
  - Cutting `PUBLICATION_SPIN_LIMIT` from `4096` to `256` / `64` reduces Point16 process CPU/Put by **-4.64% to -6.74%** (`65.41 → 62.46 / 61.04 μs`, `5/5` wins) and Point8 CPU/Put by **-4.42% to -5.20%** (`5/5` wins).
  - However, `budget_exhausted` jumps from **3.3%** (`4096`) to **52.9%** (`256`) and **60.9%** (`64`), increasing `condvar_wait_calls` by **18×–21×** (note that `condvar_wait_calls` counts user-space `Condvar::wait` entries, not necessarily the same multiplier on OS futex syscalls or `notify_all()` calls). That extra scheduling/wakeup latency trades **-2.29% to -2.60%** Point16 throughput (`0/5` wins).
  - Even at `64` spins (`39.8` mean iterations), `mvcc_cpu` remains **12.0 μs/Put** (vs `14.1 μs/Put` at `4096`), showing that the remaining ~12 μs/Put includes `Condvar` wait/wakeup, duplicate `mvcc_publication` mutex acquisition, Ready Set (`BTreeSet`) management, and Frontier advancement.

---

## 3. Cleanup Verification

- Verified local archive checksum (`ed99bd59dc8089faf731ef6e9c54eca058157ac118303e430a78571690f4a941`) before teardown.
- Deleted `walbench-mvcc-vm`, `walbench-mvcc-ssh`, and destroyed `walbench-isolated-vpc`, `walbench-isolated-subnet`, and `walbench-isolated-egress` (`Listed 0 items.` across instances, disks, walbench firewalls, and walbench networks).
