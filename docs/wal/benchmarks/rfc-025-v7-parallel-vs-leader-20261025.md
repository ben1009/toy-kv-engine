# RFC 025 Stage 8: exploratory v7 Parallel versus v7 Leader, 2026-10-25

**Status: exploratory; not a qualification result.** This comparison holds the
v7 format and PITR behavior constant while switching the WAL I/O scheduler.
Both arms use the same binary, candidate commit, host, and workloads. There
are three paired repeats per workload and filesystem, with no same-binary null
controls or confidence intervals.

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

Both arms enable PITR and explicitly select v7. The scheduler is the only
requested difference: one arm selects Parallel and the other Leader. Each
filesystem/workload cell had one unscored warmup per arm, then three pairs
ordered Parallel/Leader, Leader/Parallel, Parallel/Leader, with a fixed
3-second idle between runs. Every run used a fresh database. Put latency was
sampled every 100 puts.

PITR setup and the capacity-checked transition to v7 happen before the timed
write interval. The archive interval is one hour, and the workload stays
within one Active segment. This screen excludes setup time, rotation, archive
work, and competing archive I/O.

The [run manifest](artifacts/rfc-025-v7-parallel-vs-leader-first-leg-20261025-runs.tsv)
maps exact commands and run IDs to the original JSON stdout records in the
[JSONL artifact](artifacts/rfc-025-v7-parallel-vs-leader-first-leg-20261025.jsonl).
The [driver](artifacts/rfc-025-v7-parallel-vs-leader-first-leg-20261025-run.sh)
preserves the run order. The artifact contains 8 warmups and 24 scored records.
Its SHA-256 is
`a0e22e37458776606f00800eca19e0dac0ff3156d781569a8a091f1c09ec611b`;
the manifest SHA-256 is
`8d2b893d1b14195d2735e82e489b29e7fe64ff286707feccd8e8292518eb3b0a`.
All scored rows report v7 and the scheduler selected for that arm.

## Results

Arm throughput values are medians of three runs. Paired ratios are Parallel
divided by Leader within each pair; the median is shown after the list. A
throughput ratio above 1 favors Parallel. Latency ratios above 1 mean Parallel
is slower.

| Filesystem / writers | Leader / Parallel median puts/s | Paired throughput ratios (median) | Paired p50 ratios (median) | Paired p99 ratios (median) |
| --- | ---: | --- | --- | --- |
| tmpfs / 1 | 76,197 / 13,880 | 0.323, 0.182, 0.159 (0.182) | 2.028, 8.557, 9.051 (8.557) | 28.069, 8.557, 10.226 (10.226) |
| tmpfs / 4 | 83,423 / 64,408 | 0.727, 0.759, 0.833 (0.759) | 5.440, 2.728, 4.896 (4.896) | 0.398, 0.434, 0.423 (0.423) |
| ext4 / 1 | 1,789 / 1,641 | 0.922, 0.886, 0.917 (0.917) | 1.106, 1.131, 1.093 (1.106) | 1.288, 1.310, 1.197 (1.288) |
| ext4 / 4 | 1,796 / 6,391 | 3.558, 3.559, 10.070 (3.559) | 0.278, 0.281, 0.113 (0.278) | 0.242, 0.233, 0.071 (0.233) |

The single-writer Parallel path is slower in all three pairs on both filesystems.
On ext4, its median throughput ratio is 0.917 and its median p99 ratio is
1.288. For four writers on tmpfs, Parallel throughput is lower (median ratio
0.759), while p99 is lower (0.423) and p50 is higher (4.896). Four-writer
ext4 strongly favors Parallel in all pairs; the first two throughput ratios
are 3.558 and 3.559. The third Leader run fell to 645 puts/s from about 1,796
puts/s in the first two runs, so same-binary stability controls remain needed.

The pipeline counters show why the ext4 four-writer case benefits: Leader
records 50,000 syncs/frontiers per run, while Parallel records 12,545–12,581.
On tmpfs, Parallel records 37,595–38,511 syncs/frontiers versus 50,000 for
Leader. With one writer, both modes record 5,000 syncs/frontiers; Parallel
also submits 5,000 CQEs, while Leader submits none. Raw records retain the
per-run group, sync, frontier-byte, and latency counters.

## Interpretation and remaining gate

This screen shows a scheduler tradeoff rather than a uniform Parallel win.
Parallel has a substantial four-writer ext4 gain with fewer durability syncs,
and loses in single-writer throughput on both filesystems. The tmpfs results
also show lower Parallel throughput at four writers. These small samples lack
null controls and are not enough to qualify the scheduler or make an adoption
decision.

Stage 8 remains open. Batch64, value-size variants, rotation, checkpoints,
archive-idle and competing-archive-I/O controls, all writer counts, confidence
intervals, same-binary stability controls, hash/index CPU instrumentation,
group/coalescing coverage, and reopen/RTO measurements remain to be measured.
No adoption decision follows from this comparison.
