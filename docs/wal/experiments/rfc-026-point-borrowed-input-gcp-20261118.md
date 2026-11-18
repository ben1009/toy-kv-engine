# RFC 026 V8 Point borrowed-input experiment — 2026-11-18

## Result

Borrowing Point key/value input during V8 WAL preparation did not produce a
stable throughput gain over the existing owned-input path. The candidate is
therefore reverted from the branch's runtime source. The benchmark runner and
this report remain for provenance; no WAL format, admission, durability, or
Batch behavior change is retained.

## Method

One GCP `c4-standard-16-lssd` VM, ext4 Local SSD, same 100,000-operation
workload and 1 KiB values, with five interleaved baseline/candidate pairs per
cell. Both builds used V8 Point-only 1 MiB Lookahead, Direct FRONTIER, and
dynamic coalescing. The candidate borrowed the Point key and value for WAL
preparation instead of constructing a temporary `WalBatch` and copying those
inputs. DATA encoding, admission, frame jobs, and all Batch calls stayed on
their existing paths.

The matrix contained Point at 1, 8, and 16 writers, plus Batch16 and Batch64
at 16 writers: 25 pairs, 50 runs. All 50 runs completed successfully. The
local archive checksum and paired `result.json`/`run-metadata.json` files were
verified before cloud cleanup. The source patch SHA-256 was
`7a864782376f8bae2bd6de1e1b03c75c2ae7a21137fd60fb85439670908b4d95`.

## Results

| Workload | Writers | Owned baseline kops/s | Borrowed candidate kops/s | Paired median change | Candidate wins | CPU/put, baseline → candidate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 7.437 | 7.504 | +0.90% | 3/5 | 62.31 → 62.63 μs |
| Point | 8 | 31.921 | 31.747 | −0.55% | 0/5 | 53.28 → 53.52 μs |
| Point | 16 | 41.698 | 41.621 | −0.18% | 2/5 | 79.81 → 80.16 μs |
| Batch16 | 16 | 398.821 | 402.057 | +0.81% | 4/5 | 8.86 → 8.74 μs |
| Batch64 | 16 | 628.692 | 626.954 | −0.28% | 0/5 | 5.10 → 5.15 μs |

Sampled latency percentiles (milliseconds, baseline → candidate):

| Workload | Writers | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| Point | 1 | 0.127 → 0.126 | 0.151 → 0.151 | 0.601 → 0.581 |
| Point | 8 | 0.231 → 0.229 | 0.420 → 0.417 | 0.825 → 0.828 |
| Point | 16 | 0.345 → 0.337 | 0.770 → 0.758 | 1.413 → 1.664 |
| Batch16 | 16 | 0.612 → 0.603 | 0.920 → 0.897 | 1.741 → 1.352 |
| Batch64 | 16 | 1.654 → 1.680 | 2.458 → 2.449 | 2.753 → 2.895 |

The 16-writer Point paired observations included one +17.22% outlier, while
the other four were near zero or negative. The median paired change remained
−0.18%; it does not meet the project's 5% gain threshold. Point p95/p99 samples
also showed no consistent improvement. Batch results remained within 1%, as
expected for paths that do not use the Point-specific preparation.

## Decision and limits

Revert the borrowed-input runtime candidate. Do not proceed to direct-to-frame
encoding based on this result. The experiment only tests this GCP host,
configuration, and workload; it does not establish that borrowed input is
universally neutral. The complete local results and checksummed archives are
under `bench-results/gcp-v8-point-borrowed-input-ab-20261118/score-retry-4/`;
they are intentionally not tracked because run artifacts include environment
metadata.

The benchmark runner and its Python tests passed (114 tests). The runtime
reversion restores the tested baseline Rust files. The GCP VM and its
temporary SSH firewall were deleted and verified absent; the isolated network
was retained.
