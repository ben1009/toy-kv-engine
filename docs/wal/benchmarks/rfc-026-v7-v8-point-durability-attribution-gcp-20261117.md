# RFC 026 V7/V8 Point Durability Attribution on GCP — 2026-11-17

## Run identity

- Source commit: `2511d69dc972f155e471dc0264210e70633911c7`
- Diagnostic V7 sampler patch SHA-256: `6c38c076d1d568bf6b425cb0352a76c5aa80f50a3b5101894e4ea478da7a4586`
- Release benchmark binary SHA-256: `dd8a9cca1f4a713909ff90ab2c5ba8c20a412ca44c8885e190f74b68b6a4ff1b`
- Workload: 16 writers, 100,000 Point puts per run, 1 KiB values
- V8 arm: Point-only 1 MiB Sliding Lookahead, Direct FRONTIER, Dynamic coalescing
- Score runs: five counterbalanced V7/V8 pairs; latency sampled every 100 operations
- Diagnostic runs: one per version, with durability-phase sampling and 1,024-record commit-order tracing

The first temporary runner invocation failed before the benchmark start marker because of a shell variable error. The recovery wrapper was corrected on the retained VM; the final run then completed all 12 planned runs. No benchmark-runner or WAL runtime code was committed as part of this experiment.

## Environment and validation

The run used a GCP C4 `c4-standard-16-lssd` host with 16 logical CPUs (8 cores with SMT), an Intel Xeon 6985P-C CPU, Ubuntu 24.04.5, and Linux `7.0.0-1017-gcp`. Two Local SSD devices were combined as RAID0 and formatted ext4; the benchmark mount was `rw,noatime,stripe=256`.

The runner reports `complete: true`, 12 planned and 12 completed runs. All 12 scheduled run IDs have successful `result.json` and matching `run-metadata.json` files. The remote exit record reports success and 12 started runs. The original 94-file result archive passed its SHA-256 check locally: `754383f16ef4bb460302971758cbff5fecd46e199d5079716472005715f3272e`.

The checked-in archive omits `results/host.txt`, which contains the GCP project identifier. Its 93 remaining files retain the per-run results, metadata, schedule, environment measurements, and trace records. The [artifact manifest](artifacts/rfc-026-v7-v8-durability-attribution-gcp-20261117/artifact-manifest.json) records both original and redacted archive checksums. The [V7 diagnostic sampler patch](artifacts/rfc-026-v7-v8-durability-attribution-gcp-20261117/v7-v8-durability-phases.patch) is included so the temporary instrumentation can be reviewed.

## Unprofiled score runs

Values below are medians across five runs. Each pair ran on the same host and binary. Latency percentiles are sampled by the benchmark.

| Metric | V7 Parallel | V8 Direct/Lookahead | Paired V8 change |
|---|---:|---:|---:|
| Throughput | 48,389 puts/s | 40,794 puts/s | −16.63%; V8 won 0/5 pairs |
| Process CPU | 59.55 μs/put | 80.58 μs/put | +34.44% CPU time |
| Put p50 | 0.298 ms | 0.349 ms | +16.15% |
| Put p95 (sampled) | 0.566 ms | 0.753 ms | +41.52% |
| Put p99 (sampled) | 0.823 ms | 1.213 ms | +53.63% |
| Sync count | 8,279 | 10,042 | +20.92% |
| Puts / Sync | 12.08 | 9.96 | −17.30% |

The candidate remains slower than V7 and consumes more CPU. It also forms smaller durability groups and performs more syncs. These unprofiled paired results confirm that the remaining Point gap persists with Point Lookahead and Direct FRONTIER.

## Sampled durability phases

The diagnostic run sampled every 64 completed advances. V7 had 8,304 completed advances, 129 samples, and zero dropped samples. V8 had 10,113 completed advances, 158 samples, and zero dropped samples. Each run also captured 1,024 complete commit-order records.

Phase values are p50 unless marked p95; units are μs.

| Phase | V7 | V8 |
|---|---:|---:|
| Covered tickets / advance, p50 | 16 | 10 |
| Cycle CPU, p50 | 70.76 | 110.42 |
| Cycle wall, p50 | 175.13 | 235.02 |
| Cycle CPU per covered ticket, p50 of per-sample ratios | 6.07 | 10.87 |
| Coalescing CPU | 31.79 | 31.02 |
| Coalescing wall | 101.03 | 117.21 |
| FRONTIER preparation CPU | 4.50 | 8.62 |
| FRONTIER write CPU | 6.07 | 6.60 |
| `fdatasync` wall, p50 / p95 | 1.27 / 113.44 | 1.69 / 120.84 |
| `fdatasync` thread CPU, p50 / p95 | 1.28 / 9.97 | 1.71 / 11.90 |
| Retirement before publish CPU | 13.91 | 23.09 |
| Retirement after publish CPU | — | 17.49 |
| V8 frontier-lock wait, p50 / p95 | — | 0.81 / 11.89 |

The phase boundaries are not perfectly identical: V7 measures written-prefix to durable publication, while the V8 driver measurement begins before its frontier lock acquisition. V8 also has a separate post-publish retirement phase. The per-ticket statistic is the median of per-sample CPU/ticket ratios, not a quotient of the two displayed medians. The diagnostic values come from one instrumented run per version and include commit-order tracing. Do not add phase medians or treat them as unprofiled throughput evidence.

Coalescing CPU and FRONTIER write CPU were similar in these samples. The V8 frontier-lock wait was small. Preparation and retirement are more promising areas for follow-up, while the lower tickets-per-advance indicate that durability grouping also differs. This is diagnostic evidence, not proof that any one phase explains the throughput gap.

## Conclusion

The V8 Point candidate improves substantially over the unprepared path established in earlier experiments, but this matched run still places it 16.63% behind V7 and 34.44% higher in paired process CPU time. Coalescing CPU and lock wait do not stand out in these samples; preparation and retirement merit the next focused comparison. Any runtime change should be evaluated with the same unprofiled paired score method.

All temporary GCP resources were removed after artifact verification. The VM, boot disk, temporary SSH firewall, isolated VPC, subnet, and egress route were each confirmed absent. Terraform destroyed the three isolated network resources. The SSH recovery key and current-run scratch files were removed; the verified result archive and cleanup record remain local.

## Artifacts

- [Redacted result archive](artifacts/rfc-026-v7-v8-durability-attribution-gcp-20261117/results-redacted.tar.gz)
- [Archive manifest and checksums](artifacts/rfc-026-v7-v8-durability-attribution-gcp-20261117/artifact-manifest.json)
- [Per-run score summary](artifacts/rfc-026-v7-v8-durability-attribution-gcp-20261117/summary.tsv)
- [V7/V8 durability sampler patch](artifacts/rfc-026-v7-v8-durability-attribution-gcp-20261117/v7-v8-durability-phases.patch)
