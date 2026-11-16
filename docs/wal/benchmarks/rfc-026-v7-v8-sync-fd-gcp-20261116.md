# V7/V8 fdatasync and sync-descriptor GCP diagnostic

## Completion and provenance

- Source commit: 2042dfc2b6ca5740b3579e0639cb5c5e0b2633ae
- Runs: **15/15 successful** — 10 unprofiled score runs and 5 separate kernel-trace runs.
- Full result validation: **passed**, 15/15 planned runs; every run has a successful result.json and matching metadata.
- Machine: GCP C4 Local SSD, 16 writers, 100,000 Point puts/run, 1 KiB values.
- Kernel: Linux 7.0.0-1017-gcp. Filesystem: ext4 on the Local SSD RAID device, mounted rw,noatime,stripe=256.
- Source patch SHA-256: 7f2fec2ddbba3e17f7d720716372c7f3cb6fa9efdde49b87b61c64c17ca1c12a
- Diagnostic helper SHA-256: 7d28cb66532af2d18f9412cb1b2b56f1a8cabd6a1314fbb2ac1dcd3ddf35bf1c
- The full 173-member result archive was transferred and checksum-verified locally (SHA-256: c86425ec5f17eec3664a00c1d290d6cb3d7ae86a3fbffe8391e79bdc63830670). The raw archive is retained locally and intentionally omitted from this PR.

## Unprofiled sync-descriptor score A/B

The score runs held V8 at Buffered FRONTIER + Dynamic coalescing + 1 MiB Sliding Fallocate. Only the sync descriptor's O_DIRECT flag changed.

| Sync descriptor | Throughput median | CPU/put median | Sync count median | Put p50 median | Put p95 median |
|---|---:|---:|---:|---:|---:|
| Buffered | 31.57k/s | 70.32 μs | 6343 | 0.502 ms | 0.642 ms |
| O_DIRECT | 31.48k/s | 71.08 μs | 6340 | 0.505 ms | 0.649 ms |

Across the five counterbalanced pairs, using an O_DIRECT sync FD changed throughput by a median **-0.26%** and won **1/5** pairs. Paired CPU/put changed by a median **+0.84%**; sync counts were effectively unchanged. This gives no evidence that the sync descriptor's O_DIRECT flag is a useful throughput optimization.

## One-run kernel diagnostics

Each row below is one separate 100,000-put diagnostic run. Syscall latency is restricted to fdatasync intervals that contain an ext4 sync event for the identified WAL inode.

| Configuration | fdatasync n | <2 μs | p50 | p95 | p99 | mean | ext4/JBD2 overlap |
|---|---:|---:|---:|---:|---:|---:|---:|
| V7 Parallel | 8117 | 61.1% | 1.5 μs | 119.8 μs | 155.8 μs | 19.4 μs | 1094/8118 |
| V8 Buffered FRONTIER / buffered sync FD | 6334 | 0.0% | 172.1 μs | 292.2 μs | 512.3 μs | 186.4 μs | 6335/6335 |
| V8 Buffered FRONTIER / O_DIRECT sync FD | 6332 | 0.0% | 172.9 μs | 300.7 μs | 497.3 μs | 187.0 μs | 6333/6333 |
| V8 Direct FRONTIER / buffered sync FD | 7318 | 0.0% | 104.9 μs | 322.4 μs | 393.6 μs | 120.2 μs | 7319/7319 |
| V8 Direct FRONTIER / O_DIRECT sync FD | 7327 | 0.0% | 105.0 μs | 135.7 μs | 377.2 μs | 114.8 μs | 7328/7328 |

V7's WAL inode was inferred from ext4 events inside the benchmark's own fdatasync syscall intervals because write-perf removed its temporary database directory at exit. The selected inode had 8,117 matching syscall intervals versus 8,117 benchmark-reported syncs; 8,118 ext4 enter/exit events were observed.

For V8, wal_io_start metadata confirmed DATA, FRONTIER, and sync descriptors shared the same inode in each diagnostic run, with the planned O_DIRECT flags.

## Findings and limits

1. **The sync-FD flag is not the explanation.** With Buffered FRONTIER, changing only the sync FD from buffered to O_DIRECT left fdatasync p50/mean nearly identical (172.1/186.4 μs vs 172.9/187.0 μs) and produced no score-throughput gain.
2. **FRONTIER I/O mode changes the sync distribution.** In the single diagnostic runs, Direct FRONTIER lowered fdatasync p50 from about 172 μs to about 105 μs, and mean from about 186 μs to 115–120 μs. The Direct FRONTIER runs also had about 15% more syncs, so this is not an isolated end-to-end performance comparison.
3. **V7 still has a fast-return population that V8 lacks.** In this run, 61.1% of V7 fdatasync calls were under 2 μs; no V8 call was under 2 μs. V7's distribution was strongly mixed: 1.5 μs p50 but 19.4 μs mean and 119.8 μs p95.
4. **JBD2 activity is a correlation, not proof of cause.** A same-filesystem-device JBD2 commit overlapped 1,094/8,118 V7 ext4 sync intervals, versus every traced V8 interval. This is not a unique inode-to-transaction association.
5. Block requests and writeback events were captured systemwide and cannot be assigned uniquely to the WAL inode. Each tracing arm ran once, so p95/p99 differences are diagnostic observations, not stable rankings.

The evidence rules out changing only the sync descriptor as a meaningful optimization. It keeps FRONTIER write mode and the filesystem/JBD2 path relevant, but does not establish that device hardware alone explains the full V7/V8 gap.

## Retention

The report contains the aggregate results; the raw archive and per-run traces remain in the local experiment directory and are not committed. The diagnostic source patch and helper were also kept outside the repository. No WAL runtime code changed. I issued no VM deletion or Terraform destroy; the GCP VM remains subject to its original server-side 60-minute expiry.
