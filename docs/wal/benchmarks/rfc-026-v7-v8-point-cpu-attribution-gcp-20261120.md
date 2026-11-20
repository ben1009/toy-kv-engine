# RFC 026 V7/V8 Point CPU Attribution on GCP — 2026-11-20

- Date: 2026-11-20 UTC
- Source commit: `54cba22650e1ae198501edbb63290d48c8bb0e72`
- Binary SHA-256: `4c2fc2975356c2b4602d1f4a685bb298e85ca24480f7c986cf13c6fa3b07174c`
- Workload: C4 16 vCPU, Local SSD, ext4, Point Put, 1 KiB value, 100,000 puts; 8 and 16 writers; 3 repetitions per arm/writer (12 total).
- This suite is `diagnostic_only`; every score run had CPU attribution enabled. Treat throughput, CPU/put and sampled latency as instrumented diagnostics, not unprofiled benchmark scores.

## Run results

Run-level medians across the three repetitions; sampled p95/p99 are not full-population latency percentiles.

| Writers | Arm | Throughput k puts/s | Process CPU μs/put | Syncs | Sampled p50 ms | Sampled p95 ms | Sampled p99 ms |
|---:|---|---:|---:|---:|---:|---:|---:|
| 8 | V7 Parallel | 31.82 | 42.33 | 13040 | 0.217 | 0.430 | 0.803 |
| 8 | V8 Direct + Point Lookahead | 27.73 | 58.44 | 13285 | 0.255 | 0.564 | 1.214 |
| 8 | Paired V8 throughput vs V7 | -12.74% | +38.06% CPU/put | +2.03% | — | — | — |
| 16 | V7 Parallel | 45.42 | 60.69 | 8368 | 0.306 | 0.610 | 1.177 |
| 16 | V8 Direct + Point Lookahead | 38.53 | 87.82 | 10282 | 0.371 | 0.852 | 2.003 |
| 16 | Paired V8 throughput vs V7 | -15.17% | +43.82% CPU/put | +24.10% | — | — | — |

## Producer CPU samples

P50 across 4,686 completed selected samples per arm/writer (three runs pooled). Stage p50 values are separate distributions and must not be added.

| Writers | Arm | Producer total | Sequencer wait | Admission | Frame finish | Logical encode | Digest ready | Frame encode | Ready/submit |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | V7 Parallel | 7.52 | 0.32 | 2.10 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| 8 | V8 Direct + Point Lookahead | 22.09 | 2.83 | 2.77 | 9.25 | 2.11 | 0.75 | 0.58 | 3.19 |
| 16 | V7 Parallel | 8.93 | 0.37 | 2.34 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| 16 | V8 Direct + Point Lookahead | 29.65 | 3.42 | 3.29 | 10.65 | 2.35 | 0.88 | 0.62 | 3.70 |

At 16 writers, the V8 producer-total p50 is 29.65 μs vs 8.93 μs for V7. V8 frame-finish is 10.65 μs p50, while its subphase p50s are 2.35 μs logical encode, 0.88 μs digest-ready, 0.62 μs frame encode, and 3.70 μs ready/submit. These separate sample medians are not additive. The result points to Producer/Sequencer coordination for follow-up; it does not establish a single causal bottleneck.

## Artifact and cleanup verification

- `summary.json`: complete, 12/12 planned runs completed.
- `remote-exit.json`: failed=false, runs_started=12.
- Every planned run directory has successful `result.json` and matching `run-metadata.json`; suite validation passed after correcting the run-folder glob from `w*` to `r*` in a temporary copy.
- At the time of this run, the checked-in validator searched only `w*`, falsely reporting all `r...` run directories missing. A follow-up fix now recognizes both run-ID prefixes.
- Archive: `results.tar.gz`, 34941017 bytes; SHA-256 verified against remote metadata and sidecar: `179fda7194db8f5a75cb17c74b52c229fbce34b8d664b0125c11fa7312825c27`.
- Temporary VM, both benchmark SSH firewall rules, isolated VPC, subnet and route have been deleted and verified absent. Terraform state is empty.
