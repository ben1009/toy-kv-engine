# RFC 028 Cursor Coalescing Budget A/B

Date: 2026-11-23
Source: `ceec66ae2c3430939dac57ba9e379e1babbb2b70`
Release binary SHA-256: `57d2b9193172d1db296051b4abbb679d057dc54ed43d827a6f57f3fb1c99983e`

## Setup

- One `c4-standard-16-lssd` host; 16 vCPUs.
- Two 375 GiB NVMe Local SSDs in RAID0, ext4, mounted with `noatime` (`stripe=256`).
- 8 and 16 writers; 100,000 Point puts per run; 1 KiB values; five repetitions per arm.
- Arms: Dynamic, fixed 100 μs, fixed 400 μs. 30 total runs, counterbalanced on one host and one binary.
- Cursor + Direct Completion, Direct FRONTIER, 1 MiB Point Lookahead, singleton submission disabled. DATA used O_DIRECT; FRONTIER mode was direct; the sync descriptor was buffered.
- No CPU attribution or detailed tracing. Put latency was sampled every 10 operations (10,000 samples/run).

## Results

Values below are the median across five runs. Throughput is k puts/s; latency is ms.

| Writers | Budget | Throughput | CPU μs/put | Puts/sync | Sync count | p50 | p95 | p99 |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 8 | Dynamic | 25.75 | 52.16 | 7.25 | 13,790 | 0.245 | 0.528 | 1.688 |
| 8 | 100 μs | 25.61 | 51.66 | 7.18 | 13,921 | 0.245 | 0.523 | 1.782 |
| 8 | 400 μs | 26.13 | 50.72 | 7.96 | 12,568 | 0.243 | 0.535 | 1.639 |
| 16 | Dynamic | 26.93 | 71.36 | 9.77 | 10,231 | 0.429 | 1.930 | 3.240 |
| 16 | 100 μs | 26.61 | 71.39 | 9.40 | 10,641 | 0.430 | 1.885 | 3.232 |
| 16 | 400 μs | 27.65 | 66.58 | 15.20 | 6,581 | 0.446 | 1.670 | 2.680 |

Paired changes versus Dynamic, from the runner's paired analysis:

| Writers | Budget | Throughput change | Throughput wins | CPU μs/put change | Puts/sync change | p99 change |
|---:|---|---:|---:|---:|---:|---:|
| 8 | 100 μs | −0.41% | 2/5 | −0.75 | −0.07 | +4.82% |
| 8 | 400 μs | +1.49% | 5/5 | −1.69 | +0.71 | −2.70% |
| 16 | 100 μs | −1.14% | 1/5 | −0.33 | −0.31 | −0.34% |
| 16 | 400 μs | +2.80% | 5/5 | −4.78 | +5.42 | −17.50% |

## Conclusion

Fixed 100 μs did not help. Fixed 400 μs consistently won all five paired runs at both writer counts, reduced sync frequency, and improved sampled p99, but its throughput gain peaked at 2.80%, below the project's 5% threshold for retaining a performance change. Keep Dynamic as the default; treat 400 μs as a promising but sub-threshold candidate. No production default was changed.

Absolute throughput was about 25.6–27.7k puts/s, below earlier C4 Point runs. This suite had no concurrent V7 sentinel, so conclusions should use the same-host paired differences only; the absolute level is not comparable across dates.

## Verification and cleanup

- `summary.json`: complete, 30 planned / 30 started / 30 attempted / 30 completed.
- `remote-exit.json`: `failed=false`, 30 started and attempted.
- All 30 planned run directories contain successful `result.json` and matching `run-metadata.json`; strict result verifier passed.
- Archive: 225 files, 34,928 bytes. Remote and local SHA-256 match: `ca7f6d630814fb3a6413f3cf030443d1516dbcb447cdead9d2d5e3005272b452`. The archive is retained locally at `bench-results/rfc028-v8-coalescing-budget-20261123-verified/collection-attempt-01/results.tar.gz`; raw runs are not committed.
- GCP cleanup verified: VM, boot disk, temporary SSH rule, VPC, subnet, and route absent; Terraform state has zero resources.
- GCP runner tests: 178 passed; remote shell syntax check passed.

The runner archive allowlist initially omitted `cursor-coalescing-budget-schedule.tsv`, preventing strict local verification on the first collection. This revision includes the allowlist fix and regression test; the targeted test passed, and recollection then passed full verification.
