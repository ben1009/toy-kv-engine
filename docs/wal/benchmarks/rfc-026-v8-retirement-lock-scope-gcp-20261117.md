# RFC 026 V8 pre-publish retirement lock-scope A/B on GCP — 2026-11-17

## Run identity

- Baseline source: `ce58069d25926f38e98119558022ebc101ebcf91`.
- Candidate source: `2112ee25f7e37d59c184767aa84870a356f3af29` (`perf: move V8 retirement work outside map lock`).
- Baseline binary SHA-256: `ba8ffe9f7f1d865abea770ca2fb53ae7d1591f6927e70c7715e71971bfa48cae`.
- Candidate binary SHA-256: `5dfcb5630c2e44b4c445215e6ee338b88e356c9fb2a4fb24a965eb1de6133845`.
- Both binaries were built on the test host with `nightly-2026-09-23`; separate Cargo target directories ensured the candidate did not reuse the baseline binary.
- Workload: V8 Parallel, 16 writers, 100,000 Point puts, 1 KiB values.
- Shared configuration: Point-only 1 MiB Sliding Lookahead, Direct FRONTIER, Dynamic coalescing, no detailed profiling in score runs.
- Score schedule: five counterbalanced pairs (`AB`, `BA`, `AB`, `BA`, `AB`), 10 successful runs total.
- Diagnostics: one separate durability-sampled run per source, sampling every 64 completed advances. These runs are excluded from the score.

## Environment

The run used a GCP `c4-standard-16-lssd` host with 16 logical CPUs (8 cores with SMT), Intel Xeon 6985P-C, Linux `7.0.0-1017-gcp`, and two Local SSD devices in an ext4 RAID0 mounted with `rw,noatime,stripe=256`. Both binaries used the repository-pinned Rust nightly.

## Unprofiled score results

Values are medians across five runs per variant. CPU per put uses process user plus system CPU time. `fdatasync` is the per-run average syscall duration, then the median across runs.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Throughput | 40.90k puts/s | 40.87k puts/s |
| CPU / Put | 79.43 μs | 79.36 μs |
| Sync count | 10,044 | 10,004 |
| Puts / Sync | 9.956 | 9.996 |
| Average `fdatasync` | 21.85 μs | 21.40 μs |
| Average FRONTIER write | 38.38 μs | 38.41 μs |

Paired candidate throughput changes were **+0.36%, +1.25%, +0.60%, −0.09%, and −1.96%**. The paired median was **+0.36%**, with the candidate winning **3/5** pairs. This does not show a stable throughput improvement. Score runs did not enable latency sampling, so this experiment has no comparable p50/p95/p99 results.

## Sampled retirement diagnostics

Each row below is the p50 across samples from one diagnostic run per variant. The samples are diagnostic and are not independent benchmark repetitions; phase percentiles should not be added together.

| Phase | Baseline | Candidate |
| --- | ---: | ---: |
| Driver wall | 241.8 μs | 235.2 μs |
| Driver thread CPU | 114.4 μs | 112.5 μs |
| Pre-publish retirement wall | 24.7 μs | 26.0 μs |
| Pre-publish retirement thread CPU | 24.3 μs | 26.0 μs |
| Pre-publish resource-map lock wait | 0.234 μs | 0.287 μs |
| Pre-publish resource-map lock hold | 23.414 μs | 6.724 μs |
| Post-publish producer-guard map lock hold | 15.076 μs | 16.003 μs |

The candidate reduced the pre-publish map-lock hold p50 by about **71%**, while total pre-publish retirement CPU did not fall. Map-lock wait was already below 0.3 μs in both runs. The diagnostics therefore confirm a shorter critical section, but do not identify map-lock contention or retirement CPU as a throughput bottleneck for this workload.

The sampler recorded 158 baseline and 157 candidate advance samples, with zero dropped samples. Each run completed about 10.1k advances. Detailed per-step values are retained in the raw results archive.

## Result verification and retention

The benchmark plan and result records validate as 10 planned and 10 completed score runs. `summary.json` reports `complete: true`; `remote-exit.json` reports `failed: false` and 10 started runs. Every planned run ID has a successful `result.json` and matching `run-metadata.json`. Both diagnostic runs also completed successfully with zero dropped samples.

The raw archive is included at [the benchmark artifact](artifacts/rfc-026-v8-retirement-lock-scope-gcp-20261117.tar.gz). Its SHA-256 is `6a1f5d933791af9f4b40ba38432e19526944e858d2535e10806cdfcfb7728e6b`. It was checksum-verified locally and scanned for the GCP project ID, public IP, and private-key headers before being added.

## Conclusion

This change substantially shortens the pre-publish resource-map lock hold, but the five-pair score shows no measurable throughput gain and the diagnostic run shows no reduction in total retirement CPU. The result supports the lock-scope change as a concurrency-boundary improvement, not as a demonstrated Point performance optimization. No WAL protocol or durability ordering was changed by the experiment.
