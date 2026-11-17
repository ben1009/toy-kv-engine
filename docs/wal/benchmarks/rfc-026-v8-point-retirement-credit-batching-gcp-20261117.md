# RFC 026 V8 Point retirement credit batching A/B on GCP — 2026-11-17

## Run identity

- Baseline source: `c208ac8e55caf1e2a5231adc41967be543b92c66`.
- Candidate source: `3d7898a8e2e11970d13ed34a3f9b13241a020d62` (`perf: add V8 Point frontier credit batching experiment`).
- Candidate patch SHA-256 relative to the baseline: `5e04d110f30c8f672bf8d329c6c28cd9249b5d6c1b74832e824fa84f06afc6ef`.
- GCP release `write-perf` binary SHA-256: `c4bee96c638854fc200172feec4e522e79aeef0bbc5b725b4f403314d1b484ab`.
- Workload: V8 Parallel, 16 writers, 100,000 1 KiB Point puts.
- Shared settings: 1 MiB Point-only Lookahead, Direct FRONTIER, Dynamic coalescing. The benchmark-only `TOYKV_BENCH_V8_POINT_FRONTIER_CREDIT_BATCH` flag was the only score-run difference.
- Score schedule: five counterbalanced pairs, 10 score runs. Latency was sampled once per 100 puts. Two separate durability diagnostic runs sampled every 64 completed advances.
- Batch guardrails: one sampled `wal_batch_concurrent` diagnostic each for Batch16 and Batch64, with the experiment flag off and on.

The experiment batches unused FRONTIER-credit release for eligible all-Point durable prefixes. Batch and mixed-class prefixes retain the existing per-reservation release path. The feature is benchmark-only and defaults off.

## Environment

The tests ran on a GCP `c4-standard-16-lssd` host with 16 logical CPUs (8 physical cores with SMT), Intel Xeon 6985P, Linux `7.0.0-1017-gcp`, and Local SSD in ext4 RAID0 (`rw,noatime,stripe=256`). Both score arms used the same binary; only the benchmark environment flag changed.

## Unprofiled Point score

Values are medians across five runs per arm. CPU/put is process user plus system CPU time divided by puts. Latency percentiles are sampled and then summarized across runs.

| Metric | Batching off | Batching on |
| --- | ---: | ---: |
| Throughput | 41.014k puts/s | 41.570k puts/s |
| CPU / Put | 78.016 μs | 77.517 μs |
| Sync count | 9,845 | 9,863 |
| Puts / Sync | 10.157 | 10.139 |
| Sampled Put p50 | 0.340 ms | 0.344 ms |
| Sampled Put p95 | 0.760 ms | 0.789 ms |
| Sampled Put p99 | 3.067 ms | 2.534 ms |

Paired throughput changes were **+1.09%, +0.61%, +6.29%, +1.29%, and +1.56%**. The median paired improvement was **+1.29%**, with **5/5** pairs in favor of batching. One pair accounts for most of the gain; the other four improved by 0.6%–1.6%. This is below the previously stated 5% retention bar.

Sync count and Puts/Sync were effectively unchanged. The sampled p50 and p95 were slightly worse with batching; the p99 median was lower, but the sparse latency sample is not sufficient to claim a stable tail-latency improvement.

## Durability diagnostics

Each arm had one separate diagnostic run. The sampler recorded 158 advances with batching off and 156 with it on, with no dropped samples. Batching was observed in 151 sampled advances in the enabled run.

| Diagnostic | Off | On |
| --- | ---: | ---: |
| Pre-publish credit-release Thread CPU p50 | 6.86 μs | 3.49 μs |
| Total pre-publish retirement Thread CPU p50 | 22.70 μs | 18.92 μs |
| Tickets covered p50 | 10 | 12 |

The credit-release phase became cheaper, but these single-run diagnostic samples do not establish a corresponding end-to-end throughput cause. The different tickets-per-advance distributions also limit direct comparison.

## Batch guardrails

The Batch16 and Batch64 runs used `wal_batch_concurrent`. All four diagnostics reported zero Point-credit-batched advances, so Batch did not enter the experimental path. These were single diagnostic runs, not repeated score comparisons:

| Workload | Flag off | Flag on | Point-credit-batched advances |
| --- | ---: | ---: | ---: |
| Batch16 | 398.3k ops/s | 399.5k ops/s | 0 / 0 |
| Batch64 | 635.0k ops/s | 633.7k ops/s | 0 / 0 |

These guardrails show that the Batch path did not activate the Point-specific release operation; they are not a full mixed-load or production qualification.

## Result verification and artifacts

The corrected suite reports `complete: true`, with 16 planned and 16 completed runs. `remote-exit.json` reports `failed: false` and 16 started runs. Every planned run has a successful `result.json` and matching `run-metadata.json` in its run directory. The earlier Batch attempt used the Point workload selector by mistake; it was excluded from the result summary and retained in the archive for audit. The corrected Batch guardrails use `wal_batch_concurrent`.

The sanitized compact result archive and checksum are included at [the benchmark artifact](artifacts/rfc-026-v8-point-retirement-credit-batching-gcp-20261117.tar.gz) and [its SHA-256 sidecar](artifacts/rfc-026-v8-point-retirement-credit-batching-gcp-20261117.tar.gz.sha256). Archive SHA-256: `f46790905208ac4f0600f958950b700ad99c7616dccdc24bdab2a400fb5288f3`. The archive contains both the excluded initial attempt and corrected run set; the corrected suite is under `results/retirement-batch-20261117-corrected/`.

## Conclusion

Batching reduces the measured Point credit-release CPU, but the five-pair throughput median improves only 1.29%, below the 5% bar. The experiment does not justify enabling the optimization by default or treating it as a material Point throughput win. The benchmark-only candidate remains disabled unless further evidence warrants keeping it. Batch guardrail runs did not enter the experimental path.
