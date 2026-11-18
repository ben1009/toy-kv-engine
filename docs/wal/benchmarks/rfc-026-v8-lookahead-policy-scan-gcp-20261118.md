# RFC 026 V8 Lookahead extent-policy scan diagnostic on GCP — 2026-11-18

## Conclusion

The Point Lookahead path repeatedly scans a growing `extent_policies` map. Across five 100,000-Put V8 diagnostic runs, it made about 209,000 checks and visited about 45.04 million map entries per run: approximately 2.09 checks and 450 visited entries per Put, with the map reaching 430–431 entries.

This confirms that the full-map scan is occurring at substantial volume and justifies testing a bounded range query. This run did not test that candidate, and the counters do not isolate the CPU cost of scanning. The throughput figures below are instrumented diagnostics, not unprofiled benchmark scores.

## Run identity

- Source commit: `6ebb8e7945b93d94de15fdb038493c4ecbb36c15`.
- `write-perf` SHA-256: `b2b309ebb2c3275df399ed496cd2bfee8c85f45a0aad0b46e02f38f1216dda27`.
- GCP runner SHA-256: `2e73d8de559abcbac469b4af74fe08622f026b5d6d054fad044b794ca4ffbcd5`.
- Workload: 16 writers, 100,000 Point puts, 1 KiB values; V7 Parallel control and V8 Point-only Lookahead + Direct FRONTIER.
- Five interleaved pairs, 10/10 successful runs. V8 used 1 MiB `sliding-point-lookahead` and Dynamic coalescing. Lock diagnostics sampled every 64 operations; the Lookahead counters were enabled for the V8 runs.

## Environment

- GCP `c4-standard-16-lssd`, 16 logical CPUs (8 cores with SMT), Intel Xeon 6985P-C.
- Linux `7.0.0-1017-gcp`.
- Two Local SSD devices in an ext4 RAID0 (`/dev/md127`), mounted with `rw,noatime,stripe=256`.
- Preflight passed with the existing 60-minute lifetime and $2 estimated-spend limits. The estimate was $0.98/hour; this is not an actual billing measurement.

## Lookahead scan counters

Each row is one V8 run. The counters are exact for the instrumented Lookahead path; worker CPU is the Lookahead thread's measured Thread CPU.

| Repetition | Check calls | Map entries scanned | Maximum map entries | Worker CPU | Diagnostic throughput |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 209,350 | 45,133,601 | 431 | 68.09 ms | 40.71k puts/s |
| 2 | 208,910 | 44,960,317 | 431 | 68.67 ms | 41.15k puts/s |
| 3 | 208,830 | 44,926,832 | 430 | 65.90 ms | 40.85k puts/s |
| 4 | 209,469 | 45,092,788 | 431 | 68.51 ms | 41.13k puts/s |
| 5 | 209,273 | 45,081,578 | 431 | 68.11 ms | 40.91k puts/s |
| **Mean** | **209,166** | **45,039,023** | **430.8** | **67.86 ms** | **40.95k puts/s** |

Normalized to 100,000 puts, this is 2.09 check calls, 450.4 visited map entries, and about 0.68 μs of Lookahead worker CPU per Put. Each check visited an average of about 215 entries. Worker CPU is separate from the foreground scan cost and does not account for the additional per-entry counter updates.

## V7 control and measurement limits

The instrumented summary reported median throughput of 47.71k puts/s for V7 and 40.91k puts/s for V8. The paired median V8 change was −13.75%; V8 won one of five pairs. These runs included lock diagnostics and hot-path scan counters, so those results are not comparable to the uninstrumented V7/V8 scores from earlier reports. The diagnostic counters can themselves affect the measured path.

The run establishes scan volume, not how much of V8's CPU/Put gap the scan explains. No range-query implementation was benchmarked here. The next useful experiment is to restrict the lookup to extents that can still be unprepared, preserve the existing locking and preparation semantics, and compare the instrumented entry count plus an uninstrumented paired throughput run.

## Result verification and cleanup

The runner summary reports `complete: true` with 10 planned and 10 completed runs. `remote-exit.json` reports `failed: false` and 10 runs started. Every scheduled run has a successful `result.json` and matching `run-metadata.json`.

The verified result archive is retained locally at `/tmp/gcp-v8-lookahead-scan-6ebb8e79-run/collection-attempt-01/results.tar.gz`; its SHA-256 is `142e4a0390cda7d1361fb681dc2a9d35022568f94295eda682a0cf370cb8a00b`. The archive and checksum metadata are not added to the repository.

The benchmark VM and temporary SSH rules were removed. Terraform destroyed the isolated network, subnet, and managed SSH rule; two stale runner SSH rules were also deleted. A final GCP inventory found no matching VM, disk, network, subnet, route, or firewall rule. No steps remain incomplete.
