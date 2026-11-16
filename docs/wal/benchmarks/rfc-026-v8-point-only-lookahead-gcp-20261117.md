# RFC 026 V8 Point-Only Lookahead GCP Comparison — 2026-11-17

## Result

The corrected 40-run comparison completed on one retained `c4-standard-16-lssd` host. It compares V8 1 MiB Sliding Fallocate with a Point-only Lookahead candidate at 16 writers. There were five counterbalanced pairs for each of Point, Batch8, Batch16, and Batch64; every planned run succeeded and has a matching result and metadata file.

Lookahead improved Point throughput in all five pairs, with a median paired increase of 20.41%. Batch remained on the exact Sliding Fallocate mode in both arms, as intended. The Batch throughput differences are small and inconsistent, so the data shows no meaningful Batch change.

The Point gain comes with higher CPU use, more syncs, and worse sampled tail latency. This supports continuing the Point-only experiment, but does not qualify Lookahead as a production default.

## V8 Fallocate vs Point-only Lookahead

Throughput is the median of five runs in k operations/s. Paired delta is the median of the five within-pair percentage changes; wins count pairs with higher candidate throughput. Latency values are medians of sampled per-run quantiles.

| Workload | Fallocate k/s | Point-only arm k/s | Paired delta (wins) | CPU/op, μs | p50 / p95 / p99, ms: Fallocate → candidate | Syncs: Fallocate → candidate |
|---|---:|---:|---:|---:|---:|---:|
| Point | 32.252 | 38.621 | +20.41% (5/5) | 69.62 → 79.70 | 0.495 / 0.630 / 0.929 → 0.359 / 0.907 / 1.481 | 6,342 → 9,730 |
| Batch8 | 213.821 | 214.123 | +0.85% (3/5) | 12.69 → 12.91 | 0.593 / 0.724 / 0.862 → 0.594 / 0.730 / 0.842 | 2,379 → 2,379 |
| Batch16 | 376.000 | 378.374 | −0.32% (2/5) | 8.68 → 8.67 | 0.670 / 0.806 / 1.037 → 0.669 / 0.810 / 0.950 | 1,990 → 1,988 |
| Batch64 | 620.211 | 620.119 | −0.04% (1/5) | 4.94 → 4.92 | 1.597 / 2.143 / 2.580 → 1.617 / 2.148 / 2.487 | 1,368 → 1,363 |

The Point candidate initialized a median 405,798,912 bytes (about 387 MiB) per run. Its initialization counters report 253.8 ms of chunk work and 53.8 ms of producer preparation wait; these are accumulated diagnostic counters, not additive wall-time attribution. Point sync count increased by 53.4%, while median puts per sync fell from 15.77 to 10.28.

Point p50 improved by 27.4% in paired comparisons, but sampled p95 and p99 worsened by 41.4% and 52.3% at the paired median. Point latency is sampled every 100 operations, so the tail estimates are less precise than the throughput result and need a dedicated low-overhead confirmation.

## Point-only Batch policy

The candidate mode is selected only for Point runs. Batch8, Batch16, and Batch64 use `sliding-fallocate` in both arms; they do not enter the Point policy selector or initialize extents. This keeps the Batch runs as direct regression guardrails rather than measuring selector overhead.

An initial runner pass routed Batch candidate runs through the Point policy selector. Although it performed no initialization, those Batch results were consistently lower. That pass is excluded here. The schedule was corrected so Batch candidate runs use the same fallocate mode as baseline, then the 40-run matrix was repeated on the same retained host in separate result and database directories.

The runtime candidate remains benchmark-only and explicitly selected with `TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`. It classifies admitted DATA ranges by operation count and frame size: a single-operation, one-frame write may use Lookahead; multi-entry or multi-frame writes keep FallocateOnly. The normal production path and its defaults were not changed by this experiment.

## Setup and measurement limits

- Host: GCP `c4-standard-16-lssd`, 16 vCPUs, two Local SSDs in RAID0; Linux `7.0.0-1017-gcp`; ext4 mounted `rw,noatime,stripe=256`.
- Workloads: 16 writers, 1 KiB values; Point 100,000 operations, Batch8 300,000, Batch16 500,000, Batch64 750,000. Each workload had five counterbalanced pairs.
- Source binary commit: `d76b531b2b3a8cfd659b8b0deec56357010863ad`. The corrected schedule/runner was `8977a0d6` (`fix: keep batch guardrails on fallocate path`); the binary SHA-256 matched the prior verified build.
- Only extent mode varied for Point. The suite did not set profiling, WAL sync diagnostics, FRONTIER mode, or a coalescing override. The benchmark still collects aggregate WAL counters. Point latency is sampled every 100 operations; batch latency every 10.
- This is one-host evidence. It does not establish cross-host stability, production recovery qualification for a default-on feature, or stable p95/p99 behavior.

## Artifacts and retained environment

The corrected compact result archive remains local at `bench-results/gcp-v8-point-lookahead-20261117/corrected-run/collection-attempt-01/results.tar.gz`; its checksum metadata is beside it in `bundle-metadata.json`. SHA-256: `45aa1d801cd4eab9c8c1b2916b4f2cca0974bae30a596958981ab6d27c51f0a7`. The checksum matched the archive retained on the VM, and the complete 40-run set passed plan and per-run result/metadata validation. The archive is intentionally not committed.

The existing VM and isolated network were left untouched. The VM remains subject to its original server-side lifetime; no GCP resources were manually deleted.
