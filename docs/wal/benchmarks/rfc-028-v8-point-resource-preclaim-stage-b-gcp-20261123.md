# RFC 028 V8 Point resource preclaim Stage B GCP A/B

November 23, 2026. Tested source commit `b29ae3c663a8d098db21a7cb3b609d686625fc11`.

## Method

One `c4-standard-16-lssd` host in `us-central1-b` used two NVMe Local SSDs
striped as RAID0 and mounted as ext4. The run used the same release
`write-perf` binary for V7 Parallel, V8 Stage A, and V8 Stage A+B. The V8
configuration held Cursor progress, Direct Completion, Direct FRONTIER, 1 MiB
Point Lookahead, and Dynamic coalescing fixed. Stage A buffer preallocation
was enabled for both V8 arms; Stage B resource preclaim was the only A/B
switch. Batch workloads were not part of this matrix.

Point8 and Point16 each ran five counterbalanced rounds for V7, Stage A, and
Stage A+B. Each score run wrote 100,000 1 KiB Point values to a fresh database.
Latency was sampled every 10 puts. CPU attribution was disabled for score runs.
Eight additional diagnostics compared Stage A with Stage A+B: two rounds at
each writer count with CPU and sequencer timing sampled every 128 operations.
There were 38 planned runs in total: 30 score runs and 8 diagnostics.

## Results

Throughput and per-arm metrics are medians. Paired change is the median of the
five within-round Stage A+B versus Stage A throughput changes.

| Workload | V7 puts/s | Stage A puts/s | Stage A+B puts/s | Paired B vs A | B wins | CPU μs/put, A → A+B | Sampled p99 ms, A → A+B | Puts/sync, A → A+B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point8 | 34,905 | 34,423 | 34,169 | −3.35% | 1/5 | 47.09 → 48.88 | 0.738 → 0.732 | 7.740 → 7.731 |
| Point16 | 48,903 | 45,644 | 44,979 | −1.57% | 0/5 | 68.30 → 70.46 | 0.934 → 0.938 | 11.436 → 10.881 |

Stage B was active: each candidate score run recorded 100,000 preclaim
attempts. Median claims promoted were 71,980 at Point8 and 73,460 at Point16;
no claims were invalidated. Stage A buffer preallocation recorded 100,000 hits
and no fallbacks in both V8 arms.

The sampled sequencer-hold diagnostics did not show a consistent improvement.
At Point8, Stage A+B wall-time p50 was about 4% lower in both diagnostic pairs;
at Point16 it was 2.5% and 4.8% higher. Point16 sequencer-hold CPU p50 was also
about 3.3% and 4.1% higher with Stage B. These are two diagnostic pairs, not
performance scores.

The predeclared Stage B gate required at least a 5% paired throughput gain at
both writer counts, at least four wins out of five at each count, and no
material p99 regression. Stage B missed the throughput and win-count gates at
both counts. CPU per put increased, while Puts/sync was nearly unchanged at
Point8 and decreased at Point16. The p99 differences versus Stage A were small
and mixed. This run does not support resource preclaim as a performance
optimization; keep it disabled.

## Verification and cleanup

The completed schedule contains 38/38 runs. `summary.json` reports complete,
`remote-exit.json` reports no failure, every scheduled run has a result and
matching metadata, and local validation passed. The transferred archive hash
matches the saved remote checksum. The benchmark VM, boot disk, temporary SSH
firewall, isolated VPC, subnet, and egress route were deleted and verified
absent. No billing settings, APIs, or quotas were changed.

- Local artifact directory: `bench-results/stage-b-gcp-preparation/retry-20261123T140919Z/`
- Archive: `collection-attempt-01/results.tar.gz`
- Archive SHA-256: `cfd514beb5934892bd95d48aba1c82c65962567bcdb74875ee8075910d0d54d2`
- Benchmark binary SHA-256: `ee9aefd5127b713ad5867bb2cea5f93f468bac1af3f18095568ca4cff822f5cb`
- Remote runner SHA-256: `34ad848ccb484161dfb1176aba8d610072cc6883c453a20455986ec5d6d3c1ac`
