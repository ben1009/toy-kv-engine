# RFC 026 V7/V8 Point CPU attribution and sampler control on GCP — 2026-11-18

## Scope

This diagnostic follows the V7/V8 Point CPU attribution work in PR #558. It
measures sampled Producer and WAL-thread CPU at 1, 4, 8 and 16 writers, then
runs a same-host control with CPU attribution disabled. It changes no WAL
implementation behavior and does not qualify a production default.

The source commit was `79233c38fe92330ad7e0daf44ccd1ee22c3d7091`. The same
release benchmark binary was used for both suites; its SHA-256 was
`1d56d47fa3e199f070fb93c651a2b701da881b548aa71b24e70c7a1eb60d831b`.

Both suites used 100,000 1 KiB Point puts, compaction disabled, V7 Parallel or
V8 Parallel with 1 MiB Point-only Lookahead, Direct FRONTIER and dynamic
coalescing. The CPU attribution suite sampled producer work every 64 puts and
sampled latency every 100 puts. The control omitted
`--wal-cpu-attribution-sample-every` while keeping latency sampling at every
100 puts; its execution order was counterbalanced against the attribution
suite. There was one run per writer/version cell in each suite (8 runs per
suite).

The benchmark host was `c4-standard-16-lssd`: 16 logical CPUs on an Intel Xeon
6985P, Ubuntu kernel `7.0.0-1017-gcp`, with two Local SSDs in RAID0. The WAL
volume was ext4 mounted at `/mnt/gcp-wal-bench` with `rw,noatime,stripe=256`.
The installed Rust toolchain was `nightly-2026-09-23`.

## Throughput and process CPU

These are individual diagnostic/control runs, not repeated score medians.
Throughput is puts/s; CPU is process user+system time per Put.

| Writers | V7 attributed puts/s | V8 attributed puts/s | V7 control puts/s | V8 control puts/s |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 6,106 | 7,315 | 6,080 | 6,849 |
| 4 | 20,541 | 18,749 | 20,479 | 18,650 |
| 8 | 32,645 | 29,250 | 33,777 | 28,940 |
| 16 | 46,292 | 40,168 | 47,335 | 41,821 |

| Writers | V7 attributed CPU/Put | V8 attributed CPU/Put | V7 control CPU/Put | V8 control CPU/Put |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 71.86 μs | 60.10 μs | 72.28 μs | 62.02 μs |
| 4 | 36.80 μs | 44.66 μs | 39.40 μs | 44.70 μs |
| 8 | 40.73 μs | 56.61 μs | 40.64 μs | 51.62 μs |
| 16 | 61.42 μs | 83.40 μs | 61.58 μs | 76.46 μs |

In this single pass, V8 is faster at one writer and slower at 4, 8 and 16
writers. In the no-attribution control at 16 writers, V8 used 24.2% more
process CPU per Put and delivered 11.6% less throughput than V7. This is
consistent with a concurrency-amplified V8 cost, but one run per cell is not
enough to quantify the stable gap.

The control also shows why attributed throughput must not be presented as a
score. At 16 writers, V8 measured 83.40 μs CPU/Put and 40.17k puts/s with
attribution, versus 76.46 μs and 41.82k puts/s without it. V7 was nearly
unchanged in CPU/Put (61.42 vs 61.58 μs) and differed by 2.2% in throughput.
This suggests the attribution instrumentation may affect the V8 path more at
high concurrency. The control has only one run per cell, so it does not
establish a stable instrumentation penalty.

At 16 writers, the sampled latency values were:

| Mode | Version | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: |
| Attribution | V7 | 0.306 ms | 0.628 ms | 0.894 ms |
| Attribution | V8 | 0.350 ms | 0.820 ms | 1.417 ms |
| Attribution off | V7 | 0.309 ms | 0.561 ms | 0.810 ms |
| Attribution off | V8 | 0.339 ms | 0.721 ms | 0.896 ms |

Latency was sampled every 100 puts. These per-run percentiles are diagnostic,
not tail-latency qualification.

## Sampled CPU attribution at 16 writers

Producer phase values below are sampled Thread CPU p50 per selected Point
operation. V7 and V8 do not have identical phase boundaries; values should not
be summed to explain the process CPU/Put difference.

| Producer phase | V7 | V8 |
| --- | ---: | ---: |
| Producer total | 8.45 μs | 28.43 μs |
| Input build | 0.60 μs | 0.56 μs |
| Preflight | 1.84 μs | 1.47 μs |
| Sequencer wait CPU | 0.37 μs | 3.42 μs |
| Commit setup | 0.42 μs | 0.61 μs |
| Admission | 2.20 μs | 3.08 μs |
| Frame finish | — | 9.82 μs |
| Logical encode | — | 2.16 μs |
| Digest ready | — | 0.83 μs |
| Frame encode | — | 0.59 μs |
| Frames-ready submit | — | 3.42 μs |

V8 Producer total CPU p50 grows from 10.87 μs at one writer to 28.43 μs at
16 writers; the corresponding V7 values are 7.56 μs and 8.45 μs. The sampled
V8 producer path is therefore a strong follow-up candidate, especially its
sequencer/admission and frame-finish portions. The phase timers themselves are
inside the sampled diagnostic path, and the attribution-off control is not a
phase-by-phase control.

At 16 writers, sampled thread-role and V8 durability metrics were:

| Metric | V7 | V8 |
| --- | ---: | ---: |
| I/O worker completion CPU per work item | 1.31 μs | 2.18 μs |
| Completion reaper CPU per work item | — | 2.34 μs |
| Durability coordinator/driver CPU per work item or ticket | 6.02 μs | 10.70 μs |
| V8 durability driver CPU per advance | — | 105.24 μs |
| V8 tickets covered per advance, p50 | — | 10 |

The V7 coordinator and V8 driver work-item definitions are not identical, so
their per-item values are directional rather than a strict matched comparison.
V8 durability metrics are based on 159 sampled completed advances. Its phase
CPU p50 values were 30.80 μs for coalescing, 9.08 μs for frontier preparation,
6.61 μs for FRONTIER write, 1.72 μs for `fdatasync`, 23.21 μs for pre-publish
retirement, 17.68 μs for post-publish retirement, and 4.62 μs for coordinator
publish. These phase percentiles overlap or omit work and must not be added.

The V8 digest counters report about 2.80 μs payload-hash CPU per sampled
ticket, 0.52 μs header/batch-hash CPU, and 0.39 μs ordered-link-hash CPU. These
hash counters overlap the Producer digest-ready timing. This run does not
provide a separate Lookahead worker Thread CPU value.

## Interpretation

The no-attribution control preserves the earlier observation that V8 Point
CPU rises relative to V7 at higher concurrency. The sampled breakdown points
to additional V8 Producer CPU, I/O completion processing, Completion Reaper
work and durability-driver work. It does not isolate one stage as the cause of
the throughput gap. The current evidence supports more targeted matched
sampling; it does not justify changing the WAL algorithm, MVCC sequencer,
Group Commit policy or production defaults.

## Verification and retained artifacts

The CPU attribution runner reports `complete: true`, 8/8 runs completed and
`remote-exit.json` reports `failed: false`, 8 runs started. Each planned
writer/version cell has a successful `result.json` and matching metadata;
the runner marked all producer attribution payloads valid with no dropped
samples. The no-attribution control also has 8/8 successful result/metadata
pairs, with the attribution option absent from every command. Both suites used
the same source commit and binary SHA.

Local results are retained at:

- Attribution results: `/tmp/gcp-pr558-cpuattr-79233c38/collected/results/`
- Attribution archive: `/tmp/gcp-pr558-cpuattr-79233c38/collection-attempt-01/results.tar.gz`
- Control results: `/tmp/gcp-pr558-cpuattr-control-79233c38/collected/control-results/`
- Control archive: `/tmp/gcp-pr558-cpuattr-control-79233c38/results.tar.gz`

The attribution archive SHA-256 is
`b5079374186425bbda8b4a19c7c41209523b2275b2d4d3d7fe4b2f57929a383f`; its
local sidecar and collection metadata match the remote digest. The control
archive SHA-256 is
`9acf7a1960ebc2f6f56643543c3525163838a48683e1eb90550c2b1793b546d7`, verified
against the downloaded archive and retained sidecar. The two archives are
kept locally and are not checked into the repository.

After both suites passed checksum and per-run validation, the benchmark VM,
temporary SSH firewall, isolated VPC, subnet and egress route were removed.
A final GCP inventory and Terraform state check found zero remaining resources.
No API, billing or quota settings were changed. Cleanup proof is retained at
`/tmp/gcp-pr558-cpuattr-79233c38/cleanup-verification.json`.
