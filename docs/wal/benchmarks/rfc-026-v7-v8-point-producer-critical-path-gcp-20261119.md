# V7/V8 Point Producer Critical-Path Diagnosis

Date: 2026-11-19

Source: `daaf03c9dc4da52db32a9d847ea33ad6d74eb8e8` on `perf/v8-point-cpu-attribution`

Workload: `c4-standard-16-lssd`, 16 vCPUs, 1 KiB Point Put, 100,000 operations, 8 and 16 writers. The host ran Ubuntu 24.04.5 LTS with kernel `7.0.0-1017-gcp`. Two 375 GB Local SSDs were assembled as a 749.7 GiB RAID0 device (`/dev/md127`), formatted ext4 and mounted with `rw,noatime,stripe=256`.

The same release benchmark executable was used for V7 and V8 selection. Baseline binary SHA-256: `1d56d47fa3e199f070fb93c651a2b701da881b548aa71b24e70c7a1eb60d831b`; diagnostic binary SHA-256: `11d5e1b76d343b78d7c6504a271317af50007057279ca9991ebfefe43c9fbef5`.

## Finding

The strongest lead is contention on V8's serialized commit sequencer (`LsmMvccInner::write_lock`), which is held through commit timestamp setup and WAL ticket admission. In the sampled mutex diagnostics, V8 had substantially more sequencer-lock contention and wall wait than V7. The corresponding thread CPU wait was much smaller than wall wait, pointing to blocking/wakeup and serialized progress more than busy spinning.

V8's frame-finish path is a secondary CPU cost: at 16 writers its sampled p50 was 12.58 μs per selected Point operation. The current data does not establish how much of the clean throughput gap either path causes, so no runtime change is recommended from this experiment alone.

## Uninstrumented score runs

Five counterbalanced V7/V8 pairs per writer count. CPU attribution, mutex diagnostics, and `perf` were disabled for these score runs.

| Writers | WAL | Throughput (k puts/s) | CPU (μs/put) | Puts/sync | Put p50 / p95 / sampled p99 (ms) |
|---:|---|---:|---:|---:|---:|
| 8 | V7 Parallel | 32.175 | 40.523 | 7.681 | 0.213 / 0.428 / 0.734 |
| 8 | V8 Direct + Point Lookahead | 29.333 | 53.070 | 7.645 | 0.240 / 0.488 / 0.906 |
| 16 | V7 Parallel | 47.969 | 61.106 | 12.014 | 0.303 / 0.560 / 0.790 |
| 16 | V8 Direct + Point Lookahead | 41.236 | 78.104 | 10.023 | 0.343 / 0.742 / 1.714 |

- V8 was slower in all 5 pairs at both writer counts.
- Paired throughput change, V8 vs V7: **−10.07%** at 8 writers and **−14.04%** at 16 writers (median paired change).
- At 16 writers, V8 used about **17.00 μs more process CPU per Put** and covered fewer Puts per sync.
- Latency p95/p99 came from sampling one Put in 100. Treat these tails as directional only; five pairs do not support a firm tail-latency conclusion.

## Producer CPU attribution

One diagnostic run per arm and writer count; CPU attribution was sampled every 64 operations. These are stage p50s, not additive totals.

| Writers | WAL | Producer total CPU p50 (μs) | Sequencer wait CPU p50 (μs) | Frame Finish CPU p50 (μs) |
|---:|---|---:|---:|---:|
| 8 | V7 | 7.17 | 0.31 | — |
| 8 | V8 | 23.00 | 2.87 | 10.78 |
| 16 | V7 | 9.11 | 0.39 | — |
| 16 | V8 | 30.08 | 3.23 | 12.58 |

At 16 writers, V8's sampled Frame Finish subphases included logical encoding 2.18 μs, digest-ready 1.83 μs, frames-ready submission 5.02 μs, and DATA submission 2.90 μs p50. These phases are measured inside the broader Frame Finish span; their independent p50s do not sum to the parent p50.

## Sequencer mutex diagnostics

Twenty diagnostic runs: five repetitions for each of 8 and 16 writers, per WAL. The table shows medians across the 10 runs per arm, mixing the two writer counts rather than pooling raw samples. Lock waits were sampled every 64 acquisitions and estimated from the samples; histogram quantiles are upper bounds from log2 buckets.

| Sequencer lock metric | V7 | V8 Direct + Point Lookahead |
|---|---:|---:|
| Estimated contended acquisitions | 20.5% | 72.4% |
| Wait p50 bucket upper bound | 0.512 μs | 40.960 μs |
| Wait p95 bucket upper bound | 40.960 μs | 196.608 μs |
| Estimated accumulated wait per 100k-run equivalent | 0.418 s | 3.928 s |
| Hold p50 bucket upper bound | 4.096 μs | 6.144 μs |

The estimated accumulated wait sums waits across concurrent request threads; it is not elapsed benchmark time or CPU time. A separate 16-writer CPU-attribution run measured sequencer-wait Thread CPU p50 at 0.39 μs for V7 and 3.23 μs for V8. Together, these measurements show a large increase in serialized wall waiting, but do not alone prove that the sequencer lock is the sole throughput limiter.

The code path confirms the sequencer lock is acquired after V8 preflight and held across commit timestamp reservation, commit setup, and `admit_prepared_v8_pitr_wal_batch_only`; it is released before Frame Finish. That makes admission work inside the serialized region the next place to separate.

## Other diagnostic evidence

- V8 durability driver CPU p50 was 105.42 μs per advance at 16 writers, covering a p50 of 11 tickets (about 10.20 μs/ticket by the sampled per-ticket metric). This remains a separate candidate, not a cause established by this test.
- A single diagnostic `perf` capture contained 515 task-clock samples. Its report shows the Point write call chain through `finish_v8_frame_job`, direct-frame finish, and `publish_data`, but unresolved thread/kernel frames and the small sample count make it corroborative only. It cannot rank lock call sites reliably.
- No WAL algorithm or production code was changed.

## Recommended follow-up

Before changing synchronization, split the time spent under `write_lock` into timestamp/setup and V8 ticket admission, and compare those components at 8 and 16 writers. If admission dominates the critical section, test one safe reduction in serialized work and verify it with the same five-pair uninstrumented score matrix. Keep Frame Finish as a separate candidate. Do not infer a fix from the profiling percentages or add the independently reported phase p50s.

## Reproducibility and completion

- Planned workload: 45 runs total — 20 uninstrumented score runs, 4 CPU-attribution runs, 20 mutex-diagnostic runs, and 1 `perf` run. All 45 planned runs are present and validated.
- A first `perf` capture failed before workload launch because the disposable VM's perf permissions blocked recording; a retry after a temporary, non-persistent sysctl adjustment succeeded. The failed attempt is retained separately and is not a score.
- Diagnostic source patch: [`rfc-026-v8-point-producer-critical-path-daaf03c9.patch`](artifacts/rfc-026-v8-point-producer-critical-path-daaf03c9.patch), SHA-256
  `d30d38d5dce29ea4c920e7fa3a56c0c763f2a4550296c43926fd6c2918ef2a60`. It applies cleanly to source commit
  `daaf03c9dc4da52db32a9d847ea33ad6d74eb8e8`.
- The diagnostic binary was built after applying this patch with the repository's pinned toolchain:
  `cargo +nightly-2026-09-23 build --locked --release --package kv-engine --features bench --bin write-perf`.
  Its original-run SHA-256 is recorded above.
- Transferred archive: 37,846,589 bytes; local SHA-256 matches the remote checksum: `507b0b47652d8e3578baad14a9bb20ed05fe94b960472329646b2b4469fe429f`.
- Cleanup verification passed: VM, boot disk, temporary SSH firewall, isolated subnet/VPC, route, and Terraform state are absent.

The 37.8 MB raw archive and checksum sidecar are retained locally under
`bench-results/gcp-v8-point-critical-path-daaf03c9-20261119/`; they are not checked into this PR. The diagnostic patch is
committed at the linked artifact path so the source changes can be inspected and reapplied. The report retains the
original source, patch, binary, and archive hashes for verification.
