# RFC 024: controlled ext4 comparison, 2026-10-04

The fixed comparison completed all 114 runs in 37.7 minutes. The retained
optimized parallel WAL showed no improvement over the archived `067d4b09`
parallel executable in this session. Eight- and sixteen-writer throughput
was lower in every comparison block. All three cases failed the predeclared
full stability screen, so these measurements are descriptive and do not
qualify an optimization gain, regression size, or RFC adoption pass.

## Executables and workload

- A: archived `067d4b090f24e48477aa7322bdb025f8a69c34e8` executable, which already contains early parallel-WAL tuning. It is not the client-leader baseline or an entirely untuned implementation.
- B: latest retained optimized executable; all 107 Rust source hashes match its saved `2c4b64679add0c092325bda0831baf897494d593` provenance. Subsequent documentation changes do not change these sources.
- Binary SHA-256: A `9d4ccd2f5917a1ab02e3e15d501aeab22b50c718fafe7330beb760e77f43bcb6`; B `6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da`.
- Native io_uring, ext4 on `/dev/nvme0n1p3`, 1 KiB values, synchronous `wal_concurrent` puts, parallel WAL, PITR disabled.
- Four writers: 1 MiB SST target, exercising rotation. Eight and sixteen writers: 1 GiB SST target, exercising a single active WAL. These workloads differ, so writer-count rows are not a scaling curve.
- Every scored or identical-binary control run contains 200,000 puts; one 50,000-put warmup per arm and case.

## Frozen protocol

Six balanced ABBA/BAAB comparison blocks per writer count, with case order
rotating across rounds. Every four-run comparison block is followed by a
two-run identical-binary control at the same settings. Control arms alternate
between A and B. This gives 72 comparison runs, 36 identical-binary controls,
and six warmups. Every observation is retained; there are no selective reruns.

Each run has a fresh database. Binaries and logs remain in RAM during timing;
the databases remain on ext4. Runs are serial, with a fixed five-second idle
between them. No builds, tests, tracing, or controller admin polling overlap
timing. Owned database trees are removed after each process exits.

Within each comparison block, repeats of each arm must have throughput
max/min <= 1.05 and p99 max/min <= 1.10. Identical-binary pairs use the same
limits. Full stability additionally requires all six comparison blocks and
all six identical-binary pairs to pass, with each scored arm staying within
10% throughput spread and 20% p99 spread across the session. These limits
were frozen before the first warmup.

## Results

Paired changes below are medians of the six block ratios. Each ratio divides
the candidate geometric mean by the baseline geometric mean within that block.
The absolute columns are medians of each arm's 12 scored runs; their ratio
need not equal the paired change, especially for the unstable four-writer case.

| Writers / workload | Baseline puts/s | Candidate puts/s | Paired throughput change | Paired p99 change | Paired CPU/put change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4 / rotation | 9,827 | 9,685 | -17.6% | +123.4% | +7.1% |
| 8 / single WAL | 18,238 | 14,960 | -17.8% | +25.2% | +17.5% |
| 16 / single WAL | 27,639 | 23,495 | -14.6% | +28.5% | +11.2% |

| Writers | Absolute p99 A / B | Exploratory throughput change interval | Exploratory p99 change interval |
| --- | ---: | ---: | ---: |
| 4 | 1.389 / 1.465 ms | -44.9% to -0.4% | -4.9% to +901.7% |
| 8 | 1.123 / 1.391 ms | -18.6% to -16.7% | +21.4% to +30.7% |
| 16 | 1.311 / 1.699 ms | -16.5% to -13.8% | +17.8% to +41.1% |

Intervals are exploratory 95% block-bootstrap intervals using 20,000 resamples
of six block ratios (throughput seed 24, p99 seed 25). They do not establish
independent observations, remove SSD state changes, or override failed controls.

## Stability controls

| Writers | Comparison blocks: throughput pass | Comparison blocks: p99 pass | Identical pairs: throughput pass | Identical pairs: p99 pass | Full stability |
| --- | ---: | ---: | ---: | ---: | --- |
| 4 | 3/6 | 1/6 | 6/6 | 5/6 | Failed |
| 8 | 6/6 | 5/6 | 5/6 | 6/6 | Failed |
| 16 | 4/6 | 3/6 | 5/6 | 0/6 | Failed |

Four-writer throughput spans roughly 4,300–10,000 puts/s in both arms. In the
last block, an identical baseline changed from 4,344 to 9,913 puts/s. The
candidate also changed from 4,311 to 9,771 puts/s. Balanced ordering does not
cancel those abrupt transitions. Passing adjacent identical-binary pairs did
not guarantee stability inside the comparison blocks.

Eight-writer throughput favored A in all six comparison blocks, with B/A
ratios 0.812–0.835. Five comparison blocks passed both repeat checks, but the
final independent A/A pair changed from 17,903 to 15,686 puts/s, failing the
5% throughput limit. The failure is retained rather than waived.

Sixteen-writer throughput favored A in all six blocks, with B/A ratios
0.827–0.864. Latency remained unstable: none of the six independent identical
pairs passed the p99 screen.

## Pipeline counters

| Writers | Median sync calls A / B | Candidate sync increase | Median fdatasync time A / B | Peak in-flight groups A / B |
| --- | ---: | ---: | ---: | ---: |
| 4 | 50,917.0 / 52,103.5 | +2.3% | 13.49 / 13.76 s | 4 / 4 |
| 8 | 25,399.5 / 40,519.5 | +59.5% | 6.75 / 9.75 s | 8 / 8 |
| 16 | 13,934.5 / 21,522.5 | +54.5% | 3.68 / 5.68 s | 16 / 16 |

At eight and sixteen writers, B performs about 60% and 54% more sync calls
while reaching the same peak in-flight group count. This is consistent with
the [previous fixed-sync-cutoff investigation](rfc-024-sync-cutoff-investigation-20261003.md).
This comparison does not isolate one change or prove the controller's internal
cause. The additional syncs are a concrete software difference alongside the
storage instability.

## Artifacts and verification

Artifacts: `target/rfc024-controlled-compare-20261004-ak5kqo20/`. The frozen
protocol hash is `7817383b6ecbc6fadb9af65de7c6c31e80696f1fa2fbe238c6ddf556a9d3d558`.
The directory contains both saved binaries, source provenance, driver, raw
stdout/stderr for every run, per-run records, comparison and identical-control
records, summary, and an independent verifier.

Verification matched all 114 saved raw JSON outputs against their recorded
results, recomputed all paired medians and absolute medians, checked control
pass counts, verified unchanged protocol and binary hashes, and confirmed
owned database and RAM-stage cleanup. No production implementation changed.
