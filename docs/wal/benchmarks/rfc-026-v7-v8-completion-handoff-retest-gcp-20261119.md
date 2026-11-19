# RFC 026 V7/V8 DATA-completion handoff retest on GCP — 2026-11-19

## Scope and method

This follow-up adds the V7 Parallel completion receiver to the V8 DATA-completion timeline and corrects the receiver timestamp boundary from the earlier [V7/V8 DATA-completion timeline](rfc-026-v7-v8-data-completion-timeline-gcp-20261119.md). It changes no WAL protocol or scheduling policy.

The diagnostic used source commit `3e6f7f40cd6ad376232a4f9e93b7ce932a4363a4` and a release `write-perf` binary with SHA-256 `dc67ee2ae1f0dface0a9d9cf1adce38a91b39fb58112698c4545a2e2a77311a7`. The host was a `c4-standard-16-lssd` with two 375 GiB Local SSDs in RAID0 and ext4. Each cell ran 100,000 1 KiB Point puts with compaction disabled. The suite compared V7 Parallel against V8 Parallel with 1 MiB Point-only Lookahead, Direct FRONTIER, and dynamic coalescing at 1, 4, 8, and 16 writers: one run per cell, eight runs total.

Each cell produced 782 handoff samples at a cadence of one request per 128. No samples were dropped. These are instrumented diagnostics, not unprofiled throughput scores. Samples within one run are temporally related and do not replace repeated independent runs.

The receiver timestamp is taken after the benchmark-only enabled check and before CPU-profile initialization or timeline lookup. The sender timestamp is recorded immediately before `completion_tx.send(result)`. Therefore `send start → receiver` includes the send call, channel delivery, and receiver scheduling, but excludes the receiver profiling and timeline-lookup work that contaminated the earlier V8 measurement. The benchmark-only gate is checked lazily on the first receive so a profile configured after engine startup is observed.

The first attempt, on `91903d7d`, stopped after one V7 cell because the V7 handoff timeline had no samples; the runner rejected the incomplete set. It is excluded from the measurements below. The cause was the receiver thread caching the timeline-enabled flag before benchmark configuration, together with V7 completion results also being consumed in coalescing and drain paths. Commit `3e6f7f40` fixes those paths. The corrected attempt completed all eight cells and passed the result validator.

## Matched completion handoff

Durations are p50 / p95 / p99 in microseconds. Each row has 782 sampled completion events from one run.

| Writers | WAL | Submit → CQE observed | CQE observed → group finished | Group finished → send starts | Send starts → receiver |
|---:|---|---:|---:|---:|---:|
| 8 | V7 | 42.90 / 55.88 / 71.20 | 0.60 / 3.74 / 6.06 | 1.16 / 1.79 / 3.63 | 12.02 / 18.79 / 59.80 |
| 8 | V8 | 41.18 / 54.84 / 67.98 | 0.40 / 1.44 / 4.54 | 1.43 / 3.53 / 8.04 | 11.21 / 22.40 / 46.38 |
| 16 | V7 | 41.44 / 60.22 / 73.49 | 0.83 / 5.07 / 12.55 | 1.35 / 3.00 / 8.88 | 12.08 / 75.50 / 178.83 |
| 16 | V8 | 39.63 / 60.69 / 81.13 | 0.68 / 3.05 / 6.84 | 1.59 / 4.65 / 9.84 | 9.53 / 25.74 / 57.87 |

The send-to-receiver median does not show a V8 penalty at 8 or 16 writers. At 16 writers it is 9.53 μs for V8 and 12.08 μs for V7. V8's single-run p95 and p99 are also lower in this cell, while its submit-to-CQE p99 is higher. The 1- and 4-writer send-to-receiver p50 values were 16.11 / 15.66 μs and 11.83 / 14.18 μs for V7 / V8 respectively; the full distributions are in the local archive.

This does not establish that V8 is faster at handoff: the large V7 16-writer tail in this one capture may reflect run-specific scheduling, and the measurement is instrumented. It does reject the earlier inference that a roughly 10 μs V8 median by itself demonstrates a V8-specific reaper delay. The matched medians are similar, and the 16-writer V8 median is lower.

`Submit → CQE observed` is the time until userspace observes completion; it includes kernel completion notification and scheduling, so it is not a device-only latency. `CQE observed → group finished` and `group finished → send starts` are short at the median for both versions. The receiver interval is not pure channel transport latency.

## DATA-prefix context

The corrected V8 timeline still shows head-of-line blockers in many sampled completions: 683/782 at 8 writers and 711/782 at 16 writers. At 16 writers, 701 blockers were frame-ready and 706 had their digest folded. The blocker DATA-write age was 61.09 μs p50, 118.31 μs p95, and 276.31 μs p99 across 699 samples. This age starts before SQE submission and includes ordered-submitter queueing; it is not an SSD service-time measurement. The V7 run does not yet have equivalent per-ticket blocker-state fields.

The single instrumented 16-writer run recorded 8,364 V7 syncs and 10,244 V8 syncs, or about 11.96 versus 9.76 puts per sync. This is consistent with the earlier shorter V8 DATA-complete prefix observation, but this timeline does not prove that completion handoff causes the difference.

## Interpretation and next diagnostic

The receiver-side timestamp correction removes profiler setup and timeline lookup from the measured handoff interval. With the matched V7 data, completion send-to-receiver is not a strong V8-specific bottleneck candidate. The more useful remaining question is why a V8 ticket that is already frame-ready and digest-folded can remain the DATA-prefix blocker. A next diagnostic should capture equivalent per-ticket submission, CQE, and blocker state for V7 and V8, and distinguish kernel completion from userspace CQE observation. These results do not justify removing the completion reaper or changing Group Commit policy.

## Validation, artifacts, and cleanup

The corrected runner reported `complete: true` with 8/8 planned runs. `remote-exit.json` reported `failed: false` and 8 runs started. Every planned cell has a successful `result.json` and matching `run-metadata.json`; each handoff timeline has 782 samples. The archive SHA-256 is `096c48574b02389bb797f034d30ed93fe30ac45dbf37cc3c2618ce379a92b485` and was verified after transfer.

The archive and checksum are retained locally under `bench-results/gcp-d4d1d498-completion-handoff-20261119/`; raw results are not committed. After verification, the VM and temporary SSH firewall were deleted, Terraform destroyed the isolated VPC, subnet, and egress route, read-only inventory checks confirmed all five resources absent, and the Terraform state was empty. Temporary GCP credentials and state were removed. Billing, APIs, and quotas were not changed.
