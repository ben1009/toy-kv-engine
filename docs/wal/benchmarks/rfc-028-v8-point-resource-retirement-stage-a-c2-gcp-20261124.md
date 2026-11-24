# RFC 028 V8 Point Resource Retirement with Stage A GCP A/B — 2026-11-24

## Result

With Point buffer preallocation enabled in every Point arm, C2 batch retirement
improved paired throughput over per-ticket retirement by **4.70% at Point8**
and **1.25% at Point16**. It won 4/5 and 3/5 pairs, respectively. Against C1
detach-only, C2's incremental gains were **1.44% at Point8** and **2.46% at
Point16**, also below the 5% retention gate. The result does not qualify C2 for
default use.

Batch64 was effectively unchanged. Puts/Sync also stayed nearly constant across
retirement modes, so the small Point gains did not come from larger durability
groups. Sampled p99 was noisy: the paired median changed by −0.56% for Point8
and +1.14% for Point16 for C2 versus per-ticket retirement. There is no
consistent tail-latency improvement in this run.

## Experiment

- Source commit: `ff681f19831deda5ebdf1c98b4106b7773f4d559` on PR #562 branch
  `perf/v8-point-progress-ring-f05-qualification`.
- One release binary was used for all arms; SHA-256:
  `30e43e2af20df504b70e84648b260e5f6f5ae86e538594ebe8c099f12c9c685d`.
- Retirement modes: `0` per-ticket; `1` C1 detached Point prefix; `2` C1
  detach plus C2 batch retirement.
- Stage A Point buffer preallocation was enabled for all Point runs. Each arm
  recorded 1,000,000 attempts, 1,000,000 hits, and zero fallbacks across its ten
  Point runs. Batch64 recorded zero attempts, detached prefixes, and batched
  prefixes.
- Counterbalanced five-repetition schedule: Point8, Point16, and 16-writer
  Batch64; 100,000 operations per run. Total: 45 scored runs (15 per workload).
- Point latency was sampled once per 10 operations (10,000 samples per run).
  Batch64 latency was not sampled. No CPU attribution, mutex sampling, `perf`,
  or detailed WAL tracing was enabled.
- Fixed runtime configuration: Cursor progress mode, Direct Completion, Direct
  FRONTIER, dynamic coalescing, 1 MiB sliding Point Lookahead, ordered DATA
  submission, extent-policy Vec, Stage A preallocation enabled only for Point,
  singleton submission off, and credit batching off.
- Host: GCP C4 `c4-standard-16-lssd`, 16 vCPU, Intel Xeon 6985P-C at 2.30 GHz,
  Ubuntu 24.04, Linux `7.0.0-1017-gcp`. Two 375 GiB NVMe Local SSDs were joined
  as RAID0 and mounted as ext4 at `/mnt/gcp-wal-bench` (`/dev/md127`, 749.7 GiB).
  DATA and FRONTIER reported `O_DIRECT=true`; the sync handle reported
  `O_DIRECT=false`.
- The isolated benchmark VPC used a temporary SSH rule restricted to the
  current `/32`. The VM lifetime cap was 60 minutes; the estimate was $1.50
  against the authorized $2 ceiling. Billing, APIs, and quotas were not changed.

Per-arm values below are medians across five runs. Paired changes are medians of
the five within-repetition ratios, so they need not equal ratios of displayed
per-arm medians.

## Results

| Workload | Mode 0 throughput | Mode 1 throughput | Mode 2 throughput | Mode 0→2 paired change; wins | Mode 1→2 paired change; wins |
|---|---:|---:|---:|---:|---:|
| Point8 | 32,232 ops/s | 32,982 ops/s | 33,456 ops/s | +4.70%; 4/5 | +1.44%; 4/5 |
| Point16 | 45,429 ops/s | 46,289 ops/s | 46,027 ops/s | +1.25%; 3/5 | +2.46%; 3/5 |
| Batch64 / 16 writers | 632,093 ops/s | 632,404 ops/s | 633,261 ops/s | +0.18%; 3/5 | +0.14%; 4/5 |

| Point workload | Mode | Process CPU μs/Put | Puts/Sync | Sampled p99 ms |
|---|---:|---:|---:|---:|
| Point8 | 0 | 48.374 | 7.708 | 0.786 |
| Point8 | 1 | 46.791 | 7.702 | 0.754 |
| Point8 | 2 | 47.075 | 7.729 | 0.782 |
| Point16 | 0 | 68.860 | 11.289 | 0.960 |
| Point16 | 1 | 68.240 | 11.235 | 0.954 |
| Point16 | 2 | 68.233 | 11.299 | 0.929 |

Mode 2 versus mode 0 paired p99 changes were −0.56% at Point8 and +1.14% at
Point16. These sampled p99 comparisons are noisy and should not be treated as a
stable latency qualification.

The runtime counters confirmed the intended paths:

- Mode 0: zero detached or batched Point prefixes.
- Mode 1: 107,826 detached prefixes covering 998,232 tickets; zero batched
  prefixes across the Point runs.
- Mode 2: 107,565 detached and batched prefixes covering 998,280 tickets.
- Batch64: zero Point preallocation attempts and zero detached or batched Point
  prefixes in all three modes.

## Interpretation

- Stage A plus C1 detach-only improved paired throughput by 4.83% at Point8 and
  3.33% at Point16 versus per-ticket retirement. C2 added 1.44% and 2.46%
  against C1, respectively.
- The combined C2 result remained below the 5% retention gate at both writer
  counts and had only 3/5 Point16 wins. Do not change the runtime default based
  on this result.
- Process CPU fell by 1.30 μs/Put at Point8 and 0.63 μs/Put at Point16 for C2
  versus per-ticket retirement. Sync count and Puts/Sync were effectively
  unchanged, consistent with a small resource-retirement cost reduction rather
  than changed group formation.
- Batch64 stayed within 0.19% of mode 0. No material regression was observed.
- This is a same-host C2 qualification with Stage A enabled. It is not a V7
  comparison or a production-default qualification.

## Verification and cleanup

- `summary.json`: `complete: true`, 45/45 planned runs completed.
- `remote-exit.json`: `failed: false`, 45 runs started and attempted.
- The retirement-suite verifier accepted all 45 planned run IDs, each with a
  successful `result.json` and matching `run-metadata.json`.
- The downloaded `results.tar.gz` is 56,634 bytes and contains 335 files. Its
  SHA-256 is
  `ef9640fd60505c2896473dec0d138ed6746ac804b2d0757fb96e5022941e2361`, matching
  the remote checksum and retained `bundle-metadata.json`. Results, schedule,
  logs, archive, checksum metadata, and cleanup evidence are retained under the
  git-ignored `bench-results/v8-point-resource-retirement-stage-a-20261124-us-central1-c/`.
- The runner returned nonzero after a timeout while checking whether the
  already-deleted temporary SSH rule was absent. A follow-up inventory check
  confirmed the VM, SSH rule, isolated VPC, subnet, and route were absent;
  Terraform destroyed the two remaining network resources. Temporary SSH
  credentials and Terraform state were removed after verification.
