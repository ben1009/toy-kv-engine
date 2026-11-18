# GCP V7/V8 Point MVCC publication and sampler overhead

This follow-up measures the V7/V8 Point `publish_commit_ts()` cost and separately
checks the overhead of the benchmark-only MVCC sampler. The sample wraps the
whole publication function; it does not attribute time to its internal lock,
ready-prefix, spin, or condition-variable paths.

## Test setup

- GCP `c4-standard-16-lssd`, 16 vCPUs, `us-central1-c`, kernel
  `7.0.0-1017-gcp`, ext4 Local SSD mounted with `noatime,stripe=256`.
- 16 writer threads; 100,000 Point puts per run; 1 KiB values.
- V7 Parallel compared with V8 Parallel using Direct FRONTIER, fixed 400 μs
  coalescing, and 4 MiB sliding `fallocate`.
- io-wq was left unpinned. No `perf`, Hotpath, or WAL sync diagnostics were
  enabled. Put latency was sampled every 100 operations.
- The MVCC sampler recorded every 64th commit timestamp in both V7 and V8 runs.
  A separate V8 off/on comparison used the same settings and five counterbalanced
  pairs.

Two capacity-approved zones lacked live capacity for this C4 shape with Local
SSDs. The run completed in another zone in the same region; no billing, API, or
quota settings were changed.

## V7 versus V8

Values are medians across five runs per arm.

| Metric | V7 Parallel | V8 Direct/400 | V8 vs V7 |
| --- | ---: | ---: | ---: |
| Throughput | 48.94k puts/s | 33.12k puts/s | −32.3% |
| Process CPU per put | 59.14 μs | 71.97 μs | +21.7% |
| Put latency p50 | 0.295 ms | 0.476 ms | — |
| Put latency p95 | 0.555 ms | 0.627 ms | — |
| Put latency p99 (sampled) | 0.791 ms | 0.875 ms | — |
| Sync count | 8,205 | 6,338 | — |

V7 won all five paired throughput runs. V8's paired changes were −26.22%,
−32.21%, −32.32%, −37.22%, and −32.91%; the median paired change was −32.32%.

### `publish_commit_ts()` samples

The table shows the median of each run's sample percentile, using nearest-rank
quantiles. Each run contributed 1,562 samples with zero drops.

| Sample metric | V7 Parallel | V8 Direct/400 |
| --- | ---: | ---: |
| Wall p50 | 5.38 μs | 17.12 μs |
| Thread CPU p50 | 4.83 μs | 13.38 μs |
| Wall p95 | 93.63 μs | 96.69 μs |
| Thread CPU p95 | 59.31 μs | 57.18 μs |

V8's median publication wall and thread CPU were 3.18× and 2.77× V7. The p95
values were close. This points to a higher typical cost inside the publication
function; it does not show a V8-specific tail-wait regression or establish that
MVCC publication explains the full throughput gap.

## MVCC sampler off/on control

This separate five-pair V8 comparison changed only the MVCC sampler setting.
The sampled arm recorded every 64th commit timestamp; the off arm emitted no
MVCC samples.

| Metric | Sampler off | Sampler on | Paired change |
| --- | ---: | ---: | ---: |
| Throughput median | 33.57k puts/s | 33.26k puts/s | −1.34% median; 2/5 on runs faster |
| Process CPU per put | 71.67 μs | 72.49 μs | +1.1% by medians |
| Put latency p50 | 0.467 ms | 0.476 ms | — |
| Put latency p95 | 0.600 ms | 0.620 ms | — |
| Sync count | 6,330 | 6,327 | — |

Paired throughput changes were +5.68%, −1.34%, −2.71%, −3.77%, and +0.85%.
The result shows small, nonzero measurement overhead but is inconclusive at five
pairs. V7 and V8 used the same sampling interval in the cross-version comparison.

## What to measure next

`Mvcc::publish_commit_ts()` serializes publication through a mutex, advances a
ready prefix, and calls `await_publication()` if a predecessor is missing.
`await_publication()` first spins briefly, then waits on a condition variable.
The current samples cover all of this as one interval.

The next targeted measurement should classify calls as in-order,
out-of-order, already published, or waiting, and separately sample publication
lock wait/hold, spin iterations and exits, and condition-variable wait/reacquire.
That will distinguish the higher median CPU work from predecessor waiting
before changing the publication algorithm.

Sampling is periodic by commit timestamp, not randomized, so percentiles may
reflect deterministic scheduling patterns. The reported p95 values are
diagnostic estimates, not tail-latency qualification.

## Validation and retained data

- The V7/V8 comparison completed 10/10 planned runs; the sampler off/on control
  also completed 10/10. Every run has a successful `result.json` and matching
  `run-metadata.json`; both summaries report matching planned/completed counts,
  and both `remote-exit.json` files report success.
- All V8 runs reported stable I/O metadata: DATA and FRONTIER were `O_DIRECT`,
  the Sync FD was buffered, and sliding `fallocate` was observed.
- Both result archives passed local SHA-256 verification. Their checksums are
  `c336bdf746292c9c547235a6bc2ea22f992884b346fe1b0455d811c391cb94dc` (V7/V8)
  and `a5076f9645279df8fa287d21ba2b0f3f388fb0cabed8f9352e981112d8c5af50`
  (sampler control). The archives and checksum files remain in local working
  data; they were intentionally not added to GitHub.
- Source commit: `dd08803a56aceab3ee5c21c84c29f37e476c4542`. The isolated
  benchmark patch SHA-256 is
  `d55e26adc70d2f8e8f57bf9b4a02e7cce3bf4caa8cadd53a61c643639b14d3ff`. No
  runtime patch was applied to this PR branch.
- One initial smoke run was excluded after the collector initially looked for
  MVCC samples at the wrong JSON level. Its raw JSON and metadata were copied
  locally; the corrected formal runner reads the samples from
  `result.wal_pipeline`.
- No GCP destroy or cleanup command was run. The VM remained under its original
  60-minute server-side deletion deadline; Terraform state, VPC, subnet, SSH
  rule, and local result archives were retained.
