# RFC 026 V8 Parallel ticket-window retest (2026-11-01)

## Result

This retest did not produce comparable 64-ticket measurements. The first
32-ticket control was usable, but both 64-ticket runs and the subsequent
32-ticket control were dominated by long `fdatasync` stalls. Keep the current
32-ticket Parallel window provisionally; do not promote 64 tickets based on
the earlier single favorable sample. The retest is inconclusive about whether
64 tickets improves throughput and p99 when the device is behaving normally.

The benchmark harness also had one 32-ticket attempt fail because PITR commit
admission did not reopen within its five-second retry budget; that run produced
no measurement and is not scored.

## Method

The release `write-perf` binary with `--features bench` ran on the repository's
ext4 filesystem (`/dev/nvme0n1p3`) with 16 writers, 750,000 operations, batch
size 64, 1 KiB values, PITR V8 Parallel, no compaction, and a 2 GiB target SST
size. Latency was sampled every 10 operations. The 32-ticket control was built
from the committed source at `9bc16628`; the 64-ticket candidate changed only
`MAX_PARALLEL_INFLIGHT_TICKETS` from 32 to 64. The source was restored to 32
after building the candidate binary.

Command shape:

```bash
write-perf --bench wal_batch_concurrent --wal-batch-size 64 \
  --pitr --pitr-format v8 --wal-io-mode parallel --num 750000 \
  --threads 16 --value-size 1024 --target-sst-size 2147483648 \
  --compaction none --output json --latency-sample-every 10
```

## Runs

| Window | Run ID | Throughput (ops/s) | p50 (ms) | p99 (ms) | Max (ms) | Syncs | `fdatasync` (s) | Elapsed (s) | Classification |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 32 | `1793503148376` | 304,925 | 3.147 | 5.405 | 6.376 | 2,351 | 1.642 | 2.460 | Usable directional control |
| 64 | `1793503169075` | 54,035 | 22.326 | 49.913 | 71.363 | 1,967 | 12.920 | 13.880 | Stall-dominated |
| 64 | `1793503226417` | 27,868 | 32.208 | 87.783 | 169.857 | 1,787 | 25.848 | 26.913 | Stall-dominated |
| 32 | `1793503303011` | 53,635 | 3.740 | 83.113 | 183.776 | 2,184 | 13.050 | 13.983 | Stall-dominated control |

The later 32-ticket control also stalled, so the slow 64-ticket runs cannot be
attributed to the larger admission window from this cohort. The stall pattern
also affected the subsequent control, so these results do not isolate the
window size. The two 64-ticket results are excluded from throughput and p99
scoring because `fdatasync` consumed 93% and 96% of their measurement time.
The later 32-ticket control spent 93% of its measurement in `fdatasync` as
well.

A 100,000-operation 32-ticket diagnostic completed in 0.36 seconds, but it is
too short to qualify the full-size window comparison. It confirmed the setup
path and reports V8 Parallel counters, not steady-state performance.

## Binaries and artifacts

The control and candidate binaries were built successfully from the same
checkout, with SHA-256 digests:

```text
32-ticket control  f3da239d8e927f72b1c0b18f660bade4b29bd2c7ad85d7a37272e0942f37a326
64-ticket candidate 58d60cc05fc19958010336f2ef608d585f587708dae3e0a0bed76e92d7e84687
```

Raw JSONL, stderr, and binaries remain under the ignored local directory
`target/rfc-026-v8-ticket-window-retest-20261101/`. The large temporary
benchmark databases were removed after the inspection.

## Decision

No code change is retained. Keep 32 tickets until a repeated paired ext4 screen
can score both throughput and p99 without storage-stall contamination.
