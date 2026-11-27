# Issue #570 MVCC Publication Final Acceptance (Baseline A vs Candidate D)

- **Date**: 2026-11-27
- **Baseline A**: `ea65d941` (`main`; `PUBLICATION_SPIN_LIMIT = 4096`, broadcast wakeup; sha256 `752bf27a43592f8b938c26283403f12e43ad50b6c0cf3e805412e814397fd99d`)
- **Candidate D**: `99b75ac0f21012836b861bd48fe41673ebfd45e5` (`perf/mvcc-publication-step2a`; spin `256` + Step 2A + Targeted Wakeup + P2/P3 fixes; sha256 `be6512b9e4cecb03cca68d5e1484c0529d5615b19ab8aa5b14c85b42c99661d2`)
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2x NVMe Local SSD RAID0, `write-perf --features bench` (diagnostics off), `--num 100000 --value-size 1024 --compaction none --latency-sample-every 100`. V8 runs use `--pitr-format v8` with sliding-point-lookahead extent prep and direct FRONTIER; V7 runs use `--pitr-format v7`.
- **Design**: 6 workloads x 5 counterbalanced pairs (`A D / D A / A D / D A / A D`) = 60 runs.
- **Local archive**: `bench-results/gcp-mvcc-final-20261127/final-ab.tar.gz` (sha256 `a65ccfe70bba503d3c4f260fdfe656bd02150932b403bcebd4279f5bddbda9da`, 60/60 JSONs present; checksum verified against the hash printed on the VM, since the separate `.sha256` copy failed with an SSH timeout).

## 1. Results (paired medians, D vs A)

| Workload | A ops/s | D ops/s | Throughput | CPU/Put (A -> D) | CPU delta | P99 delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| V8 Point1 | 8,718 | 8,637 | +0.35% (3/5) | 50.97 -> 51.17 us | +0.36% (2/5) | -19.90% (4/5) |
| V8 Point8 | 37,784 | 38,207 | +1.15% (4/5) | 42.37 -> 40.20 us | -4.40% (5/5) | -6.83% (4/5) |
| **V8 Point16** | 54,124 | 53,116 | **-2.35% (0/5)** | 64.84 -> 60.36 us | **-6.64% (5/5)** | -12.60% (3/5) |
| V8 Batch16/w16 | 423,756 | 425,976 | +0.82% (3/5) | 7.86 -> 7.51 us | -5.01% (5/5) | -17.34% (4/5) |
| V8 Batch64/w16 | 631,606 | 631,413 | -0.26% (2/5) | 4.64 -> 4.56 us | -0.25% (3/5) | -7.98% (3/5) |
| V7 Point16 | 48,766 | 47,845 | -1.77% (0/5) | 59.87 -> 52.97 us | -11.27% (5/5) | -2.49% (3/5) |

## 2. Against the proposed acceptance criteria

| Criterion | Result | Verdict |
| --- | --- | --- |
| Point16 CPU down at least 5% | -6.64% (5/5) | Pass |
| Point16 throughput regression at most 2%, check P99 | -2.35% (0/5); P99 -12.60% (3/5) | **Not met** (0.35 pt over, every pair lost) |
| Point1 / Point8 / Batch no clear regression | Within +/-1.2%; CPU down on Point8/Batch16 | Pass |
| V7 no clear regression | Throughput -1.77% (0/5), CPU -11.27% (5/5) | Pass on throughput threshold; same direction as V8 |

The Point16 throughput loss is small but consistent (0/5 pair wins in both V8 and V7), so it should be treated as a real trade-off and not noise. The earlier 4-arm run measured D at -1.65% vs A; this run measured -2.35%. Both are between-run estimates and the criterion is a proposed engineering threshold, not a previously agreed one.

## 3. Cleanup

- Deleted `walbench-final-vm`, `walbench-final-ssh`, and destroyed `walbench-isolated-vpc`, its subnet and egress route; see the PR comment for the verification listing.
