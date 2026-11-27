# Issue #570 MVCC Publication Step 2A (`4096` vs `256` vs `256 + Step 2A`)

- **Date**: 2026-11-27
- **Issue / Branch**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / `perf/mvcc-publication-step2a`
- **Arms tested** (unprofiled `write-perf --features bench`, Point16 `100k` puts, `1024 B` value):
  - **Arm A (`4096 orig`)**: `ea65d941` (`main`, sha256 `752bf27a43592f8b938c26283403f12e43ad50b6c0cf3e805412e814397fd99d`)
  - **Arm B (`256 orig`)**: `564eb72b` (`PUBLICATION_SPIN_LIMIT = 256`, sha256 `f88f7df0bad42fe12cbc461f22148535a2d6bcfb70e8651b9c9139e60999ee63`)
  - **Arm C (`256 + Step 2A`)**: `88814f99` (`finish_publication_locked` under `Condvar` lock, sha256 `d9503978739f2f9a7be933769aff9b11435d5c83e35ad2a8e36decb330c3fc71`)
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/gcp-wal-bench`, `ext4`), `TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`.
- **Local archive**: `bench-results/gcp-mvcc-step2a-20261127/step2a.tar.gz` (`sha256: 1e328197820c637f3acdf5b38c675c29f553847bd377825e4bbbfb6934029985`, 15 unprofiled Point16 runs + 3 diagnostic runs verified).

---

## 1. Point16 Counterbalanced A/B/C Results (5 Triples, Unprofiled)

| Arm | Ops/s (Median) | Process CPU/Put (Median) | P50 / P99 (Median) | Paired Delta vs A (`ops/s` · `CPU/put`) | Paired Delta vs B (`ops/s` · `CPU/put`) |
| --- | ---: | ---: | ---: | ---: | ---: |
| **A (`4096 orig`)** | 53,246 | 64.90 μs | 0.256 / 0.892 ms | — | — |
| **B (`256 orig`)** | 52,582 | 62.20 μs | 0.264 / 0.975 ms | -0.98% (1/5) · **-4.09% (5/5)** | — |
| **C (`256 + Step 2A`)** | 51,935 | **61.18 μs** | 0.267 / 0.923 ms | -2.12% (0/5) · **-6.08% (5/5)** | -1.15% (0/5) · **-2.21% (5/5)** |

---

## 2. Diagnostic Verification (`--mvcc-publication-sample-every 64 --mutex-lock-sample-every 64`)

> [!NOTE]
> Diagnostic runs only (`bench-results/gcp-mvcc-step2a-20261127/step2a-diag`); not scored as unprofiled throughput.

| Arm | `ops/s` (diag) | `CPU/put` (diag) | `mvcc_publication` acq | `est_contended` | `wait_ms` (wall) | `acq / waited` | `mvcc_cpu` (all / waited mean) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **A (`4096 orig`)** | 53,181 | 66.21 μs | 169,107 | 67,732 | 244.26 ms | 2.04 | 13,378 / 19,970 ns |
| **B (`256 orig`)** | 52,658 | 62.14 μs | 222,436 | 45,154 | 453.11 ms | 2.86 | 12,771 / 17,722 ns |
| **C (`256 + Step 2A`)** | 52,038 | **61.83 μs** | **166,048 (-25.4%)** | **39,360 (-12.8%)** | 492.69 ms | **2.00** | **11,978 / 16,080 ns** |

- **Interpretation**:
  - Step 2A successfully eliminated the 3rd `mvcc_publication` mutex acquisition on the `Condvar` wait path (`222,436 → 166,048` acquisitions, `acq_per_waited` `2.86 → 2.00`), cutting Point16 process CPU/Put by another **-2.21% (`5/5` wins)** vs Arm B (`62.20 → 61.18 μs`, **-6.08%** vs Arm A).
  - However, eliminating the 3rd lock did **not** recover the `256` vs `4096` Point16 throughput gap (`-1.15%` vs B, `-2.12%` vs A, `0/5` wins). The remaining throughput loss comes from `Condvar` wait + `notify_all()` thundering-herd wakeup latency (`wait_ms` `244.26 → 492.69 ms`), pointing directly to **targeted per-timestamp / frontier-covered waiter wakeups** (separating normal publication waiters from PITR barrier drain) as the next step.

---

## 3. Targeted Publication Wakeup 4-Arm A/B/C/D Results (`a3c59293`, Point16, 5 Quadruples)

- **Arms tested**:
  - **A (`4096 + Orig + Broadcast`)**: `ea65d941` (sha256 `752bf27a43592f8b938c26283403f12e43ad50b6c0cf3e805412e814397fd99d`)
  - **B (`4096 + Step 2A + Broadcast`)**: `6b30a537` with `PUBLICATION_SPIN_LIMIT = 4096` (sha256 `3d49bf357740b601ffee575b5d7f305fb893718cb2ff7b742d046105d56678d9`)
  - **C (`256 + Step 2A + Broadcast`)**: `6b30a537` (sha256 `0cd3e76b7dd71f710cfe9104067bb85dc66e3841c7b81ee7dad4e34a13779221`)
  - **D (`256 + Step 2A + Targeted`)**: `a3c59293` (sha256 `4df6edb32169143b7c0fa573528e7c01f15b8d6db4094dda046e61161843ca36`)
- **Local archive**: `bench-results/gcp-mvcc-targeted-20261127/targeted.tar.gz` (`sha256: 0c831dd22ee8f7b4dba852d2c6abfd1ff52bfbefe0fd50645c83d771a2a304b6`, 20 unprofiled runs + 4 diagnostic runs verified).

### 3.1 Unprofiled Point16 Results (5 Counterbalanced Quadruples)

| Arm | Ops/s (Median) | Process CPU/Put (Median) | P50 / P99 (Median) | Paired Delta vs A (`ops/s` · `CPU/put`) | Paired Delta vs C (`ops/s` · `CPU/put`) |
| --- | ---: | ---: | ---: | ---: | ---: |
| **A (`4096 + Orig + Bcast`)** | 53,690 | 65.13 μs | 0.259 / 0.927 ms | — | — |
| **B (`4096 + 2A + Bcast`)** | **54,292** | 64.75 μs | 0.250 / 1.034 ms | **+1.12% (3/5)** · -0.57% (3/5) | — |
| **C (`256 + 2A + Bcast`)** | 51,573 | 60.65 μs | 0.268 / 1.141 ms | -3.21% (0/5) · **-6.15% (5/5)** | — |
| **D (`256 + 2A + Targeted`)** | 53,205 | **60.58 μs** | 0.263 / 1.039 ms | -1.65% (1/5) · **-7.43% (5/5)** | **+2.71% (5/5)** · +0.12% (2/5) |

### 3.2 Diagnostic Verification (`--mvcc-publication-sample-every 64 --mutex-lock-sample-every 64`)

| Arm | `ops/s` (diag) | `CPU/put` (diag) | `mvcc_publication` acq | `est_contended` | `wait_ms` (wall) | `mvcc_cpu` (all / waited mean) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **A (`4096 + Orig + Bcast`)** | 53,565 | 64.44 μs | 168,937 | 64,274 | 208.36 ms | 13,166 / 19,548 ns |
| **B (`4096 + 2A + Bcast`)** | 54,094 | 65.50 μs | 165,909 | 66,517 | 286.89 ms | 14,290 / 20,323 ns |
| **C (`256 + 2A + Bcast`)** | 52,630 | 61.05 μs | 166,133 | 39,078 | 468.28 ms | 11,530 / 15,303 ns |
| **D (`256 + 2A + Targeted`)** | 52,905 | **61.08 μs** | 220,286 | **36,340** | **96.18 ms (-79.5% vs C)** | **9,373 / 12,158 ns** |

- **Key result**: Targeted wakeup (`D`) recovered **+2.71% (`5/5` wins)** Point16 throughput over `256 + 2A + Broadcast` (`C`, `51,573 → 53,205/s`), slashed `mvcc_publication` lock wait from **468.28 ms → 96.18 ms (-79.5%)**, cut `mvcc_cpu` from **13,166 ns → 9,373 ns (-28.8%)**, and kept Point16 process CPU/Put at **60.58 μs (-7.43% vs A, `5/5` wins)**.

## 4. Cleanup Verification

- Verified local archive checksums (`1e328197820c637f3acdf5b38c675c29f553847bd377825e4bbbfb6934029985`, `0c831dd22ee8f7b4dba852d2c6abfd1ff52bfbefe0fd50645c83d771a2a304b6`) before teardown.
- Deleted `walbench-step2a-vm`, `walbench-step2a-ssh`, `walbench-targeted-vm`, `walbench-targeted-ssh`, and destroyed `walbench-isolated-vpc`, `walbench-isolated-subnet`, and `walbench-isolated-egress` (`Listed 0 items.` across instances, disks, walbench firewalls, and walbench networks).
