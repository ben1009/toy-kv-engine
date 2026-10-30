# RFC 025 Stage 8: exploratory v7 Leader versus v6 Leader, 2026-10-25

**Status: exploratory; not a qualification result.** This comparison holds
Leader scheduling and PITR enabled while comparing the v7 embedded-frontier
format with v6. Both arms use the same binary, candidate commit, host, and
workloads. There are three paired repeats per workload and filesystem, without
same-binary null controls or confidence intervals.

## Executable and environment

- Candidate commit: `f08f4cd7f0bac424d9732837401a89a76ec5b5b6`.
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

Both arms enable PITR and explicitly select v6 or v7. Both explicitly select
Leader I/O. Each filesystem/workload cell had one unscored warmup per arm, then
three pairs ordered v6/v7, v7/v6, v6/v7, with a fixed 3-second idle between
runs. Every run used a fresh database. Put latency was sampled every 100 puts.

PITR setup and the v7 capacity-checked transition occur outside the timed write
interval. The archive interval is one hour, and each run stays within one
Active segment. This screen excludes setup time, rotation, archive work, and
competing archive I/O. The [run manifest](artifacts/rfc-025-v7-leader-vs-v6-leader-20261025-runs.tsv)
maps exact commands and run IDs to the original JSON stdout records in the
[JSONL artifact](artifacts/rfc-025-v7-leader-vs-v6-leader-20261025.jsonl). The
[driver](artifacts/rfc-025-v7-leader-vs-v6-leader-20261025-run.sh) preserves
the run order. The artifact contains 8 warmups and 24 scored records. Its
SHA-256 is
`5051328db0b44f1b6db4225c99fe748a17dfda9e11be3a3c4b08943d1d630836`; the
manifest SHA-256 is
`1e9869a290c590142b361c5af857c2d6a83fbd5cae8bcf44a182189977c56bc4`. All
scored rows report the requested format and Leader mode.

## Results

Arm throughput values are medians of three runs. Paired ratios are v7 divided
by v6 within each pair; the median is shown after the list. A throughput ratio
above 1 favors v7. Latency ratios above 1 mean v7 is slower.

| Filesystem / writers | v6 / v7 median puts/s | Paired throughput ratios (median) | Paired p50 ratios (median) | Paired p99 ratios (median) |
| --- | ---: | --- | --- | --- |
| tmpfs / 1 | 55,981 / 80,587 | 1.491, 1.120, 1.482 (1.482) | 0.831, 1.064, 0.713 (0.831) | 0.105, 0.169, 0.153 (0.153) |
| tmpfs / 4 | 171,452 / 75,141 | 0.510, 0.451, 0.392 (0.451) | 0.632, 1.193, 0.643 (0.643) | 4.491, 2.531, 4.636 (4.491) |
| ext4 / 1 | 1,836 / 1,804 | 0.985, 0.988, 0.980 (0.985) | 1.017, 0.998, 1.019 (1.017) | 1.026, 1.051, 1.010 (1.026) |
| ext4 / 4 | 3,750 / 1,798 | 0.484, 0.191, 0.475 (0.475) | 1.989, 4.193, 2.009 (2.009) | 2.449, 7.711, 2.327 (2.449) |

For four writers, v7 throughput is lower in all three pairs on both
filesystems. The median paired throughput ratios are 0.451 on tmpfs and 0.475
on ext4. The ext4 v7 second pair is an outlier at 715.7 puts/s; it is retained
in the table and has not been excluded. Single-writer ext4 is near parity in
this small sample, while single-writer tmpfs favors v7.

The WAL counters show the format cost under Leader scheduling. In single-writer
runs, both versions issue 5,000 syncs, while v7 writes 5,000 4 KiB frontiers
(20,480,000 bytes). At four writers, v7 issues 50,000 syncs/frontiers and
204,800,000 frontier bytes per run; v6 issues about 25,000 syncs on tmpfs and
24,000 on ext4. The WAL file is also nearly twice as large with v7: 20,975,616
versus 40,980,480 bytes for single-writer runs, and 205,524,992 versus
409,620,480 bytes for four-writer runs.

## Interpretation and remaining gate

This exploratory screen does **not** show performance parity across the tested
workloads. It shows near-parity single-writer throughput on the physical device,
but a large four-writer regression for v7 Leader on both filesystems. The
frontier and sync counts indicate that v7 Leader is not coalescing four-writer
durability advances as effectively as v6 Leader in these runs. The three-pair
sample, missing null controls, and retained ext4 outlier limit the conclusion;
they do not establish a stable regression magnitude or an adoption decision.

Stage 8 remains open. Batch64, value-size variants, rotation, checkpoints,
archive-idle and competing-archive-I/O controls, all writer counts,
same-binary stability controls, confidence intervals, hash/index CPU
instrumentation, group/coalescing coverage, and reopen/RTO measurements remain
to be measured. The candidate v7 Parallel versus baseline v6 Leader comparison
and the remaining qualification evidence are still needed. No adoption
decision follows from this comparison.
