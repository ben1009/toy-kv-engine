# RFC 028 V8 Dedicated Durability Driver GCP A/B — No broad throughput win, 2026-11-24

## Result

The dedicated durability driver did not produce a broad performance improvement.
It was consistently faster for Point8, but the gain was small; Point1 regressed
substantially, and Point16 was slightly slower in four of five paired runs.
Keep caller-driven durability as the default. This run does not qualify the
dedicated driver for production use.

## Experiment

- Source commit: `edcec9ac597f12a15f7bfcd13e85b5ea1407f9fe`
- Release `write-perf` SHA-256: `5ff9bcc7f030fee8972d265aac43e4946e016293b117b6509fea49e68ae8486a`
- Host: GCP `c4-standard-16-lssd`, 16 vCPUs, zone `us-central1-b`
- Storage: two NVMe Local SSDs, RAID0, ext4 mounted at `/mnt/gcp-wal-bench`
- Schedule: 40 unprofiled scoring runs (five interleaved pairs for Point1,
  Point8, Point16, and Batch64) plus four driver-counter diagnostics
- Workload: 100,000 operations per run; 1 KiB Point values; Batch64 uses 64
  entries per operation
- Fixed configuration: Cursor progress mode, Direct Completion, dynamic
  coalescing, direct FRONTIER, ordered DATA submission, 1 MiB Point lookahead,
  Point buffer preallocation enabled, and resource-retirement mode 0
- Only the CallerDriven/Dedicated durability-driver mode varied. Detailed
  dedicated-driver counters were disabled in scoring runs. The four diagnostic
  runs are excluded from performance comparisons.

The first capacity check passed in `us-central1-a`, but GCP reported a C4
Local-SSD stockout when creating the VM. No VM or SSH firewall was created in
that attempt. A fresh capacity check and suite preflight passed in
`us-central1-b`; all results below come from that single host.

## Scoring results

Throughput, CPU, latency, and Puts/Sync columns show the median of five runs per
arm. Paired throughput and p99 changes are the medians of the five within-round
percentage changes. Latency values are milliseconds.

| Workload | CallerDriven ops/s | Dedicated ops/s | Paired throughput change | Dedicated wins | CPU/Put CallerDriven → Dedicated | p99 CallerDriven → Dedicated | Paired p99 change | Puts/Sync CallerDriven → Dedicated |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point1 | 8,274.6 | 7,185.1 | −12.20% | 0/5 | 54.19 → 83.03 μs | 0.426 → 0.436 | +2.29% | 1.00 → 1.00 |
| Point8 | 33,518.6 | 34,286.9 | +2.29% | 5/5 | 49.10 → 49.40 μs | 0.786 → 0.739 | −7.24% | 7.69 → 7.68 |
| Point16 | 43,743.8 | 43,308.7 | −1.18% | 1/5 | 70.89 → 72.38 μs | 1.002 → 1.011 | +4.93% | 11.17 → 10.99 |
| Batch64 / 16 writers | 626,771 | 630,557 | +0.75% | 4/5 | 5.017 → 5.021 μs | Not sampled | — | 515.46 → 512.82 |

Point8's 5/5 wins and improved p99 are encouraging but small. The Point1 CPU
cost and throughput regression, together with the lack of a Point16 win, mean
the result does not support enabling a separate durability thread generally.
Group size did not improve: Puts/Sync was nearly unchanged at Point1/8 and
slightly lower for Dedicated at Point16 and Batch64.

## Driver diagnostics

The separate diagnostic runs confirmed that the mode switch and wake
coalescing worked. For each 100,000-Point run, CallerDriven recorded 100,000
driver-election attempts. Dedicated recorded zero caller elections and 100,000
requests. At 8 writers, the driver sent 19,241 wake signals, coalesced 80,759
requests, and made 13,011 advance attempts. At 16 writers, it sent 4,426 wake
signals, coalesced 95,574 requests, and made 8,927 advance attempts. These
instrumented runs are diagnostic only and are not used as throughput scores.

## Verification and artifacts

- `summary.json`: `complete: true`, 44/44 runs completed (40 scoring, 4
  diagnostic)
- `remote-exit.json`: `failed: false`, 44 runs started and attempted
- Exact-schedule result verifier: passed; all 44 run directories have successful
  `result.json` and matching `run-metadata.json` and environment metadata
- Transferred archive: `results.tar.gz`, SHA-256
  `e03794b0d0a1ce45292675c16ce65b5c25be70f4ecc23a2a91027e900e5413c0`
- Local archive and checksum metadata:
  `bench-results/gcp-v8-d3a-20261124-us-central1-b-run/collection-attempt-01/`
- Extracted results:
  `bench-results/gcp-v8-d3a-20261124-us-central1-b-run/collected/results/`
- Cleanup: VM and `/32` SSH firewall were absent after the run; Terraform then
  destroyed the isolated VPC, subnet, and egress route. Independent GCP describe
  checks confirmed all five resources absent. No default-VPC rule was changed.
- Cost guard: $1.50/hour estimate, 60-minute maximum runtime, $2 maximum
  estimated-spend ceiling. This is a guard, not a billing cap; actual billed
  spend was not queried.

The bundle manifest records matching remote and local archive checksums. The
report's absolute and per-run data remain in the ignored local `bench-results`
directory; only the compact summary and archive checksum are committed here.
