# RFC 025 Stage 8: exploratory v7 Parallel versus v4 Parallel, 2026-10-24

**Status: exploratory; not a qualification result.** This is the second RFC 025
comparison: current v7 Parallel with PITR enabled against current v4 Parallel
with PITR disabled. Both modes use the same `write-perf` binary, host, and
workloads. The screen has three paired repeats per workload and filesystem,
without same-binary null controls or confidence intervals.

## Executable and environment

- Both arms used candidate commit
  `f08f4cd7f0bac424d9732837401a89a76ec5b5b6`.
- Binary SHA-256:
  `d12902ba53e16f4a5c3b18dde6b2fd758169140e14ea9b6ab98f3f369c927023`.
- Build command:
  `cargo build --locked --release --package kv-engine --bin write-perf --features bench`
  with `rustc 1.100.0-nightly (6bb1652a0 2026-09-22)`.
- Host: x86_64 Linux 6.12.71-1-lts, Intel Core i9-13900T, 32 logical CPUs.
- Storage: `/dev/shm` tmpfs (32 GiB); ext4 on `/dev/nvme0n1p3`, the
  Solidigm SSDPFKNU010TZ.

## Protocol

| Workload | Configuration |
| --- | --- |
| Single-writer WAL puts | 5,000 puts, 1 writer, 1 KiB values, 1 GiB SST target |
| Four-writer WAL puts | 50,000 puts, 4 writers, 1 KiB values, 1 GiB SST target |

Both arms use Parallel I/O. The v7 arm enables PITR and explicitly requests
v7; the v4 arm disables PITR. Each filesystem/workload cell had one unscored
warmup per arm, then three pairs ordered v4/v7, v7/v4, v4/v7, with a fixed
3-second idle between runs. Every run used a fresh database. Put latency was
sampled every 100 puts.

The harness bootstraps the PITR repository, enables PITR, and performs the
capacity-checked v6-to-v7 transition before starting the timed write interval.
The configured archive interval is one hour, and the workload stays within one
Active segment. This measures steady-state WAL/PITR write cost; it excludes
PITR setup time, rotation, archive work, and competing archive I/O.

The [run manifest](artifacts/rfc-025-v7-vs-v4-first-leg-20261024-runs.tsv)
maps exact commands and run IDs to the original JSON stdout records in the
[JSONL artifact](artifacts/rfc-025-v7-vs-v4-first-leg-20261024.jsonl). The
[driver](artifacts/rfc-025-v7-vs-v4-first-leg-20261024-run.sh) preserves the
run order. The artifact contains 8 warmups and 24 scored records. Its SHA-256
is `5ae1488c897274045d5588c6658c2f4322a919378c5549379e064947d38e528f`;
the manifest SHA-256 is
`51e4ea84601ec1284588b48be02b0950c444ee5fcd9496fb636fb3f071d0d14f`.
All scored rows report the requested WAL format and Parallel I/O mode.

## Results

Ratios are v7 divided by v4 within each pair. For throughput, a ratio below 1
is slower; for p50 and p99 latency, a ratio above 1 is slower. Arm throughput
columns are medians of three runs; paired ratios are listed in run order with
their median in parentheses.

| Filesystem / writers | v4 / v7 median puts/s | Paired throughput ratios (median) | Paired p50 ratios (median) | Paired p99 ratios (median) |
| --- | ---: | --- | --- | --- |
| tmpfs / 1 | 34,797 / 19,926 | 0.743, 0.537, 0.695 (0.695) | 1.264, 1.972, 1.243 (1.264) | 0.813, 0.910, 1.102 (0.910) |
| tmpfs / 4 | 161,160 / 64,360 | 0.436, 0.397, 0.381 (0.397) | 2.778, 2.616, 3.144 (2.778) | 0.681, 1.333, 1.102 (1.102) |
| ext4 / 1 | 752 / 452 | 0.621, 0.556, 1.337 (0.621) | 1.391, 1.993, 0.620 (1.391) | 1.182, 1.164, 1.296 (1.182) |
| ext4 / 4 | 9,206 / 6,222 | 0.653, 0.688, 0.679 (0.679) | 1.540, 1.484, 1.523 (1.523) | 1.090, 1.338, 1.144 (1.144) |

The four-writer throughput reduction is consistent across the three pairs:
the median paired ratio is 0.397 on tmpfs and 0.679 on ext4. The single-writer
ext4 ratios range from 0.556 to 1.337, so that case remains sensitive to device
variation. Four-writer ext4 p50 and p99 are higher in every v7 pair.

The v7 pipeline counters show the additional in-WAL metadata traffic. At one
writer, each 5,000-put run records 5,000 frontier writes (20,480,000 frontier
bytes) and 5,000 syncs. At four writers, tmpfs records 34,870–36,906 frontier
writes and 142,827,520–151,166,976 frontier bytes; ext4 records 12,574–12,596
frontier writes and 51,503,104–51,593,216 frontier bytes. In these runs,
frontier writes and sync counts match, so the DATA and FRONTIER records share
one sync per durability advance as specified.

## Interpretation and remaining gate

This small steady-state screen shows a substantial four-writer throughput
cost for v7 Parallel with PITR enabled, along with higher p50 latency. The
ext4 single-writer case is noisy, and the screen lacks same-binary controls;
the results are preliminary and do not by themselves make an adoption decision.
The reported difference covers the v7/PITR write path as a whole. This harness
does not expose separate hash or index CPU timings, so it cannot attribute the
throughput gap to any one component.

Stage 8 remains open. Batch64, value-size variants, rotation, checkpoints,
archive-idle and competing-archive-I/O controls, all writer counts, confidence
intervals, same-binary stability controls, hash/index CPU instrumentation,
group/coalescing coverage, and reopen/RTO measurements remain to be measured.
No adoption decision follows from this comparison.
