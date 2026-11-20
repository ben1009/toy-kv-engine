# V7/V8 Point Frame Finish to DATA submission on GCP

Source commit `63f2c9ecc50d74acb1cc4fa6af0f1984fa285fb3` with a temporary diagnostic patch. All 12 runs completed and the result archive passed checksum and run validation. The test used a single C4 16-vCPU host, two NVMe Local SSDs in RAID0 with ext4, and three counterbalanced V7/V8 pairs at each writer count. Each run issued 100,000 Point puts with 1 KiB values and sampled every 128th operation. V8 used 1 MiB Point-only Lookahead, Direct FRONTIER, and Dynamic coalescing.

| Writers | V7 puts/s | V8 puts/s | V7 CPU/put | V8 CPU/put |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 34,041 | 30,937 | 39.99 µs | 52.07 µs |
| 16 | 48,288 | 41,529 | 60.88 µs | 78.21 µs |

The table reports medians across instrumented runs. At 16 writers, the median of run p50s was 10.46 µs thread CPU for frames ready through submission, 4.83 µs while holding the submission lock, and 3.91 µs in `drain_ready_locked`. The coordinator, guard map, and submission lock acquisition waits had wall p50s of 0.28, 0.27, and 0.23 µs respectively. The measured phases overlap and include instrumentation overhead. V7 has no directly comparable breakdown for these V8 stages.

The data points to work inside the submission lock as the largest measured substage. An uninstrumented A/B would be needed to establish whether reducing that work improves throughput.

Two earlier launches stopped at SSH readiness. The successful run refreshed the SSH source /32 and used a dedicated VPC. The VM and temporary SSH rule were verified absent after collection, and Terraform destroyed the temporary network resources.

## Local experiment artifacts

The result archive and diagnostic patch are retained locally. The archive includes the benchmark plan, summary, completion status, and per-run results and metadata.

| Artifact | Path relative to `/tmp/gcp-v8-frame-finish-20261120/` | SHA-256 |
| --- | --- | --- |
| Verified results archive | `fresh-results/collection-attempt-01/results.tar.gz` | `5e1dd816bb3597f33d822d346e2c7dc8d087ed70f83ba1a95d80fd55d8ea70f5` |
| Diagnostic source patch | `frame-finish.patch` | `01270c26d73b06ecb03ed3d5c0f0361198e53b83c3c751572a60c551f946c17b` |

## Follow-up submission breakdown

A second diagnostic used the same source baseline, hardware shape, workload, and sampling interval. All 12 runs completed and the archive passed checksum and run validation. There were 2,343 V8 producer samples at each writer count. The instrumentation accumulated measurements across any groups submitted by each sampled producer call.

| V8 stage | 8 writers wall p50 / p95 / p99 (µs) | 8 writers CPU p50 / p95 / p99 (µs) | 16 writers wall p50 / p95 / p99 (µs) | 16 writers CPU p50 / p95 / p99 (µs) |
| --- | --- | --- | --- | --- |
| Pending collection | 0.31 / 0.47 / 0.62 | 0.31 / 0.48 / 0.63 | 0.35 / 0.62 / 1.11 | 0.35 / 0.62 / 1.13 |
| Extent readiness, credit and write packing | 1.52 / 2.77 / 6.36 | 1.54 / 2.78 / 5.13 | 1.61 / 3.22 / 11.30 | 1.62 / 3.24 / 7.84 |
| WriteGroup construction | 0.28 / 0.36 / 0.41 | 0.29 / 0.36 / 0.44 | 0.30 / 0.40 / 0.57 | 0.31 / 0.40 / 0.68 |
| Worker submission | 2.71 / 4.61 / 8.19 | 2.72 / 4.62 / 8.19 | 2.71 / 4.97 / 8.85 | 2.72 / 4.99 / 8.86 |
| Worker slot acquisition and capacity wait | 0.38 / 0.56 / 1.39 | 0.39 / 0.56 / 1.39 | 0.39 / 0.58 / 1.19 | 0.40 / 0.58 / 1.18 |
| Worker channel send and conditional wake | 1.56 / 3.37 / 6.18 | 1.57 / 3.38 / 6.22 | 1.50 / 3.53 / 6.42 | 1.51 / 3.53 / 6.44 |

Values are medians of per-run percentiles. Worker slots and send/wake are nested inside worker submission. The packing vector counter counts initial allocations and capacity growth for the temporary vectors in the caller. Its p50 was two per sampled call at both writer counts, with one submitted ticket at p50. No sampled group hit worker slot capacity.

Worker submission was the largest measured stage, with send and conditional wake accounting for its largest measured substage. Pending collection and group construction each took about 0.3 µs CPU at p50. These results do not justify moving submission outside the lock or selecting a packing optimization for an A/B. The measurements contain timer overhead and do not establish what fraction of the CPU can be removed.

At 16 writers, median diagnostic throughput was 48.10k/s for V7 and 41.58k/s for V8, at 60.74 and 78.13 µs process CPU per Put respectively.

- Local archive `/tmp/submit-breakdown-run.GWkdM1/results/collection-attempt-01/results.tar.gz`, SHA-256 `6b5bb192a2d430c3f2845b87b82cbf053cef06ef8acdd40de502109f5c75a246`.
- Local diagnostic patch `/tmp/gcp-v8-submit-breakdown.patch`, SHA-256 `3d73c8c406cffffa015153b03166e46a94a3f5743cddaac00a0c6288f55d8506`.
