# RFC 024 WAL Baseline

This records the current client-leader `wal_concurrent` control before any
parallel scheduling change. It is the Slice 1 baseline for the
[RFC 024 implementation plan](rfc-024-parallel-wal-implementation-plan.md),
not evidence that the proposed pipeline improves performance.

## Run manifest

- Source revision: `3a64397f861d7d98462ff3c350413cc3e5f82b26`
- Build: `cargo build --release --features bench --bin write-perf`
- Rust: `rustc 1.100.0-nightly (6bb1652a0 2026-09-22)`
- Cargo: `cargo 1.100.0-nightly (495c385d0 2026-09-16)`
- Release binary SHA-256: `766e4a78abe728e8ed5da620dbab812acec9203533958f4edc94e3c4c4be5ca3`
- Runtime environment: `HOTPATH_METRICS_SERVER_OFF=true`
- WAL mode: `leader`; WAL enabled; PITR disabled
- Workload: `wal_concurrent`, 200,000 puts, 4 writers, 1 KiB values,
  1 MiB target SST size, seed 42
- Latency sampling: every 100th put per writer, 2,000 samples per run

All sampled runs used the Slice 1 selector, profile counters, and latency
sampling support from the worktree. They used the same release binary and
arguments. The binary hash was checked before and after the run series and
matched. The WAL scheduling path remained the existing client leader; no
parallel worker or candidate write I/O was active.

Exact benchmark arguments, used for both filesystems:

```text
--suite legacy --preset default --wal --bench wal_concurrent
--num 200000 --threads 4 --value-size 1024 --target-sst-size 1048576
--latency-sample-every 100 --profile --output json
```

The runs were performed on 2026-09-25. `/tmp` was tmpfs. The repository WAL
path was on ext4 backed by `/dev/nvme0n1p3`, a SOLIDIGM SSDPFKNU010TZ NVMe SSD
(953.9 GiB). The ext4 benchmark directory was removed by the harness.

## Same-binary paired current-path results

Four control pairs were collected with this binary and workload. Filesystem
order was tmpfs then ext4 in pairs 1, 2, and 4, and ext4 then tmpfs in pair 3.
Pair 1 was the initial sampled run; pairs 2–4 were rerun after review feedback.
The exact WAL paths were:

| Pair | First run | Second run |
| --- | --- | --- |
| 1 | tmpfs: `/tmp/rfc024-baseline-tmpfs-sampled` | ext4: `.rfc024-baseline-ext4-sampled` |
| 2 | tmpfs: `/tmp/rfc024-pair-01-tmpfs` | ext4: `.rfc024-pair-01-ext4` |
| 3 | ext4: `.rfc024-pair-02-ext4` | tmpfs: `/tmp/rfc024-pair-02-tmpfs` |
| 4 | tmpfs: `/tmp/rfc024-pair-03-tmpfs` | ext4: `.rfc024-pair-03-ext4` |

Ext4 paths in this table are relative to `/home/liu/proj/toy-kv-engine`.
The harness removed each run's temporary benchmark directory.

| Pair | WAL filesystem | Measured time | Throughput | p50 put | p99 put | WAL groups | Solo groups | `fdatasync` total | Preallocation | Follower wait |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | tmpfs | 1,337.44 ms | 149,540 ops/s | 0.017427 ms | 0.178858 ms | 92,178 | 18,805 | 18.91 ms | 148.57 ms | 2,959.88 ms |
| 1 | ext4/NVMe | 90,353.04 ms | 2,214 ops/s | 1.132102 ms | 11.930945 ms | 86,134 | 15,927 | 55,062.48 ms | 35.56 ms | 265,320.44 ms |
| 2 | tmpfs | 1,224.63 ms | 163,315 ops/s | 0.016853 ms | 0.173530 ms | 94,716 | 19,360 | 19.39 ms | 140.64 ms | 2,652.91 ms |
| 2 | ext4/NVMe | 93,717.38 ms | 2,134 ops/s | 1.139746 ms | 11.438966 ms | 87,080 | 16,311 | 55,534.35 ms | 37.78 ms | 273,920.39 ms |
| 3 | ext4/NVMe | 55,003.69 ms | 3,636 ops/s | 1.077671 ms | 2.677279 ms | 87,407 | 16,526 | 30,510.51 ms | 33.26 ms | 159,385.65 ms |
| 3 | tmpfs | 1,273.80 ms | 157,010 ops/s | 0.016833 ms | 0.172755 ms | 92,200 | 16,651 | 17.73 ms | 144.38 ms | 2,764.95 ms |
| 4 | tmpfs | 1,401.99 ms | 142,654 ops/s | 0.018885 ms | 0.219914 ms | 92,947 | 19,802 | 20.97 ms | 155.04 ms | 3,129.52 ms |
| 4 | ext4/NVMe | 54,599.62 ms | 3,663 ops/s | 1.074656 ms | 2.688992 ms | 86,815 | 15,564 | 30,625.85 ms | 33.49 ms | 159,061.97 ms |

The per-filesystem medians across four runs are:

| WAL filesystem | Measured time | Throughput | p50 put | p99 put | WAL groups | Solo groups | `fdatasync` total | Preallocation | Follower wait |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tmpfs | 1,305.62 ms | 153,275 ops/s | 0.017140 ms | 0.176194 ms | 92,574 | 19,083 | 19.15 ms | 146.48 ms | 2,862.42 ms |
| ext4/NVMe | 72,678.37 ms | 2,925 ops/s | 1.104887 ms | 7.063979 ms | 86,948 | 16,119 | 42,844.17 ms | 34.53 ms | 212,353.05 ms |

Each run completed 200,000 write CQEs and reported 819,200,000 committed WAL
bytes. Every run reported a maximum of one in-flight group, four outstanding
write SQEs, one group per sync, and zero dedicated-worker wakeups.
`outstanding_write_sqes` is software pipeline depth, not block-device queue
depth.

The device-backed measurements have substantial spread: throughput ranged
from 2,134 to 3,663 ops/s and sampled p99 ranged from 2.68 to 11.93 ms. Treat
these four pairs as a current-path reference, not a performance claim or an
adoption-gate result. The final RFC gate still requires candidate/control
pairs, a same-session null pair, and a pre-PITR reference.

## Pre-instrumentation cross-check

Before the Slice 1 changes, the same revision and workload were run once on
each filesystem with an uninstrumented release binary (SHA-256
`986752947510b57d0481434363831dbe626ace3e0d8ee951996f61f898d697b2`). Those
runs did not collect latency samples:

| WAL filesystem | Measured time | Throughput | WAL groups | Solo groups | `fdatasync` total | Follower wait |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tmpfs (`/tmp`) | 1,340.82 ms | 149,162 ops/s | 93,873 | 21,934 | 18.48 ms | 2,874.52 ms |
| ext4 (`/dev/nvme0n1p3`) | 93,901.26 ms | 2,130 ops/s | 85,896 | 17,828 | 57,272.41 ms | 274,817.05 ms |

These are independent single runs, so their differences from the sampled runs
must not be attributed to instrumentation or used as a performance comparison.
The later RFC benchmark gate still requires same-binary, paired, alternating
runs, including a null pair and the pre-PITR reference.
