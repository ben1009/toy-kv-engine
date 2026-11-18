# RFC 026 V8 Sliding Lookahead Full-Matrix GCP Comparison — 2026-11-16

## Result

The complete 256-run matrix finished successfully on one `c4-standard-16-lssd` host: V4 Parallel, V7 Parallel, V8 Parallel with 1 MiB Sliding Lookahead, and V8 Leader; 1/4/8/16 writers; Point, Batch8, Batch16, and Batch64; four repetitions per cell. All planned run IDs have a successful `result.json` and matching `run-metadata.json`; `summary.json` reports `complete: true` and 256/256, `remote-exit.json` reports `failed: false`, and matrix validation reports no missing, duplicate, unexpected, invalid, or failed cells.

This qualifies the Lookahead candidate across the matrix but does **not** support enabling it globally. It improved the median in 9/16 writer/workload cells and regressed 7/16. At 16 writers it was slightly ahead on Batch8/Batch16, remained 13.9% behind V7 on Point, and was 47.5% behind on Batch64. The substantial Batch64 decline is a qualification failure for a general-purpose/default path.

## Matched V7 vs V8 Sliding Lookahead

Four repetitions per cell. Throughput is median k operations/s. Delta is the median of four within-repetition V8/V7 ratios. p99 is sampled by the benchmark (every 100 Point operations and every 10 batch operations). CPU is process CPU per operation, derived per run before taking the median. Sync count is the per-run median.

| Writers | Workload | V7 k/s | V8 Lookahead k/s | Paired delta | V8 wins | CPU/put V7 → V8 (μs) | Sampled p99 V7 → V8 (ms) | Syncs V7 → V8 |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | point | 6.07 | 6.93 | +15.1% | 4/4 | 70.5 → 61.7 | 0.310 → 1.705 | 100000 → 100000 |
| 1 | batch8 | 41.23 | 47.31 | +15.1% | 4/4 | 11.6 → 9.8 | 0.320 → 0.878 | 37500 → 37500 |
| 1 | batch16 | 70.25 | 82.94 | +18.1% | 4/4 | 7.1 → 5.9 | 1.014 → 1.064 | 31250 → 31250 |
| 1 | batch64 | 146.85 | 182.34 | +24.4% | 4/4 | 4.1 → 3.2 | 0.806 → 1.222 | 11719 → 11719 |
| 4 | point | 22.10 | 20.02 | -8.3% | 0/4 | 37.9 → 42.5 | 0.434 → 0.977 | 25050 → 25140 |
| 4 | batch8 | 117.62 | 115.53 | -0.8% | 1/4 | 9.2 → 9.6 | 0.696 → 0.972 | 9466 → 9500 |
| 4 | batch16 | 181.83 | 191.78 | +5.5% | 4/4 | 7.0 → 6.4 | 0.857 → 1.118 | 8167 → 7978 |
| 4 | batch64 | 386.99 | 311.96 | -19.4% | 0/4 | 4.4 → 3.8 | 1.238 → 2.029 | 4477 → 3472 |
| 8 | point | 33.80 | 29.73 | -11.7% | 0/4 | 40.2 → 51.1 | 0.763 → 1.104 | 13038 → 12930 |
| 8 | batch8 | 159.86 | 171.58 | +7.5% | 4/4 | 10.7 → 10.4 | 1.404 → 1.283 | 6889 → 5156 |
| 8 | batch16 | 247.62 | 253.31 | +1.8% | 4/4 | 7.3 → 7.0 | 1.417 → 1.788 | 6263 → 4810 |
| 8 | batch64 | 485.22 | 313.61 | -35.4% | 0/4 | 4.6 → 4.3 | 1.585 → 3.036 | 1774 → 2660 |
| 16 | point | 46.98 | 40.45 | -14.9% | 0/4 | 59.5 → 77.0 | 0.878 → 1.917 | 8246 → 9795 |
| 16 | batch8 | 203.32 | 209.39 | +2.7% | 3/4 | 13.0 → 13.6 | 1.704 → 2.078 | 4239 → 3842 |
| 16 | batch16 | 256.96 | 263.42 | +2.4% | 4/4 | 8.3 → 9.2 | 2.378 → 2.479 | 3734 → 3400 |
| 16 | batch64 | 601.82 | 316.00 | -47.5% | 0/4 | 5.1 → 5.3 | 3.182 → 5.248 | 1224 → 1534 |

For 16 writers specifically, paired median throughput changes were Point −14.9% (0/4 wins), Batch8 +2.7% (3/4), Batch16 +2.4% (3/4), and Batch64 −47.5% (0/4). The 16-writer Batch64 V8 runs report about 823 MiB initialized per run, roughly 1.53–1.59 s initialization elapsed, and higher cumulative frontier-write/fdatasync counters than V7. These counters overlap and are diagnostic context, not additive wall-time attribution; they suggest the initializer's I/O cost should be examined for large batches.

## Setup and measurement limits

- Source commit: `d58674fec79bbe608135486a1c979227e0227b78`.
- Experimental runner patch SHA-256: `76afc527210f4c24781252e5468ff55bf924bcc3be3c903996502e4a43f2f98e`. It only enables `sliding-lookahead` with a 1 MiB window for V8 Parallel and records that mode; all 64 V8 Parallel runs' environment files confirm it.
- Host: GCP `c4-standard-16-lssd`, 16 logical CPUs / 8 cores, Xeon 6985P-C; two 375 GiB Local SSDs in RAID0; Ubuntu 24.04, kernel `7.0.0-1017-gcp`; ext4 mounted `rw,noatime,stripe=256`.
- No `perf` or Hotpath profiling was enabled. The harness did capture its 100 ms WAL timeline in every run, so these are not zero-instrumentation scores. p99 values are sampled, not full-operation distributions.
- The first process safely stopped at 228 runs due its VM-deadline guard. The 28 remaining cells were resumed on the same VM and disk without reformatting or changing benchmark parameters. Resumed runs used a 45-second timeout cap; none hit it. The summary was regenerated from all per-run results after the resume helper's summary import-path issue.
- This is one-host evidence and does not imply cross-machine or filesystem qualification. It compares V7 and the V8 Lookahead candidate, not same-run V8 Fallocate vs Lookahead.

## Artifact and environment retention

The compact result archive is retained locally at `collection-attempt-01/results.tar.gz` with checksum metadata at `collection-attempt-01/bundle-metadata.json`. SHA-256: `e7fcb4fa39aaf31d87f1aafa9d6e3e118bf934c32bbb97d5c21a0e8f1ce6b962` (210128 bytes). The checksum matches the remote archive, and the archive passed the runner's full per-run verification. Timeline JSON files were omitted from this compact archive; raw benchmark JSON, run metadata, commands, stderr, plan, schedule, summaries, and host metadata are retained.

The result archive and per-run JSON remain in the local ignored `bench-results` directory; no archive was uploaded to GitHub. The VM reached its configured 60-minute server-side expiry (`2026-11-16 14:14:59 UTC`). At report time, the temporary SSH rule, isolated VPC/subnet, and Terraform state were retained pending explicit teardown.
