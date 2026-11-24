# V8 Point D1 ACK A/B

## Result

D1 did not produce a throughput gain in this run. Point8 was effectively flat, and Point16 was slower in all five paired comparisons. CPU per Put fell by about 1% at both Point concurrency levels, but that did not translate into better throughput. The result does not support retaining D1 as a performance optimization.

The tested change is the consolidated Coordinator ACK check in candidate `5d2873f8c44134ccc329eb11b5ec6a495ee86bb7`, compared with baseline `dca60652e0d5401aa5b17081c6fc52d36974380b`.

## Measurements

Five counterbalanced pairs per cell; medians across runs. Point latency is sampled every 10 operations. The runs had no CPU attribution or detailed profiling enabled.

| Workload | Baseline throughput | D1 throughput | Paired throughput change | D1 wins | Baseline CPU/Put | D1 CPU/Put | Paired p99 change |
|---|---:|---:|---:|---:|---:|---:|---:|
| Point8 | 34,955 Put/s | 34,892 Put/s | −0.18% | 1/5 | 48.73 μs | 48.23 μs | +1.82% |
| Point16 | 45,730 Put/s | 45,092 Put/s | −1.86% | 0/5 | 70.09 μs | 69.37 μs | +3.11% |
| Batch64 / 16 writers | 631,962 Put/s | 630,956 Put/s | −0.29% | 1/5 | 5.03 μs | 5.02 μs | n/a |

At Point16, Puts/Sync rose slightly from 11.30 to 11.44, while median p99 rose from 0.916 ms to 0.956 ms. One paired p99 observation was a large outlier (+98.17%); the paired median change was +3.11%.

All Point runs recorded 100,000 preallocation hits and zero fallbacks. Batch64 recorded zero attempts, hits, and fallbacks. Retirement mode was 0 throughout.

## Environment and identity

- GCP C4, 16 vCPUs, `us-central1-c`; two 375 GiB NVMe Local SSDs in RAID0.
- Benchmark filesystem: ext4 on `/dev/md127`, mounted at `/mnt/gcp-wal-bench` with `noatime`.
- Same VM and pinned Rust toolchain (`nightly-2026-09-23`) for both revisions.
- Baseline binary SHA-256: `2655e23fb86ecdf4f1df227fdcdf9b5555ed2ae51a4c3897a6a4dc21a34b2aff`.
- D1 binary SHA-256: `5ab07dac13f19c71645763c26b4dd1fe26ba2e69559e93bcdeedff56e9d5b323`.
- Exact baseline-to-candidate Rust patch SHA-256: `0646d981b8769e51952608e1f8a5a692930d09fdb73d53bfa9e8ed4e4244b662`.

## Validation and cleanup

The complete 30-run matrix passed the D1 verifier: 30 planned, started, attempted, and completed; every run has matching `result.json` and `run-metadata.json`. The transferred 236-file archive matched the VM-reported SHA-256 `bda727463a4ac48ed81c27911d941eb26ca770e89f816aa6e0a7a7b889bb18ab`.

The first orchestration attempt stopped before any score run because the local patch hash used a custom Git diff formatter. The already-retained VM was reused; the actual archived patch was verified and the full matrix then completed. The local D1 verifier was corrected to accept the Batch64 runner's unset preallocation variable as the default-off state; Batch64 counters independently confirm that the path was not entered.

The VM, temporary SSH firewall, isolated VPC, subnet, and egress route are all confirmed absent. Terraform state and retained SSH credentials were removed after verification. No default VPC rule was used or changed.

## Artifacts

- Raw, verified run set: `collected/results/`
- Original verified archive: `collection-attempt-01/results.tar.gz`
- Archive and run verification: `verified-results.json`
- Cloud cleanup verification: `cloud-resource-verification.json`
- Detailed metrics: `collected/results/summary.json`
