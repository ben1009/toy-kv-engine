# RFC 025: tracing point-write handoffs, 2026-10-29

**Finding:** in two excluded retained-policy traces, DATA submission-return-to-CQE-reap is the largest mean interval: **843 us at one writer and 768 us at eight writers**, about 67% and 50% of mean API latency. Submission itself averages only 6–10 us. Publication-to-caller-return averages 10–54 us. This argues against targeting the final notification first; it does not distinguish physical device latency, kernel completion work and reaping/scheduling delay.

The follow-on [notification experiment](rfc-025-v7-signal-taskwork-20261029.md) tests one source hypothesis. Its 16-writer batch guard is inconclusive, so no runtime optimization is retained.

## Fixed excluded captures

The [plan](artifacts/rfc-025-v7-point-handoff-20261029-plan.json) precedes instrumentation/capture. A single instrumented retained-policy executable runs point1/20,000 puts/sample10, then point8/100,000 puts/sample100. Both use v7 Parallel, repository-local ext4, 1-KiB values, a 2-GiB SST target, no compaction, a thirty-second initial cooldown and fifteen-second gaps. There are no replacements or performance qualification.

Temporary instrumentation records every API call, its interior `wait_durable` ticket, DATA staging/submission/reap, packer/coordinator checkpoints and durable publication. A bounded global RAM buffer timestamps with CLOCK_MONOTONIC and serializes insertion with a mutex; no trace event is dropped. The API event buffer is written after engine close, outside the timed workload. The existing RAM-buffered native interposer records synchronous WAL writes and syncs at process exit. A 10-ms observer samples thread states and scheduling counters, with a 1-Hz host audit. No competing recognized workload is detected; no build, test, source edit, profile or SSD health polling overlaps captures.

Trace locking, allocation/page faults, timestamps and the observer can change scheduling/grouping. These are diagnostic invocations, not scored throughput estimates. Kernel scheduler statistics are disabled (`sched_schedstats=0`), so runqueue counters cannot establish runnable-delay attribution. The recorded cgroup scopes have no CPU quota and report no throttling; that does not exclude unrelated interference.

## Exact call paths

All **20,000 / 100,000 API calls** map to exactly one WAL wait ticket, DATA request and covering native FRONTIER/sync cycle. Each path's intervals are nonnegative and sum exactly to its traced API duration. Group endpoints map packer events to their individual tickets; native FRONTIER targets locate the first covering durability cycle. The source keeps its retained grouping and one-sync-before-ACK rules.

| Mean interval, us | 1 writer | 8 writers |
| --- | ---: | ---: |
| API entry → packer | 19.0 | 42.7 |
| Packer materialization/preparation | 46.5 | 19.8 |
| Packer ready → worker staging | 12.3 | 14.2 |
| Staging → submission return | 5.7 | 9.9 |
| Submission return → CQE reap | 843.0 | 768.2 |
| CQE reap → covering FRONTIER | 24.1 | 90.6 |
| FRONTIER write | 20.9 | 93.0 |
| FRONTIER end → sync start | 1.0 | 1.0 |
| fdatasync | 263.8 | 395.3 |
| Sync end → publication | 2.9 | 13.4 |
| Publication → waiting caller return | 10.2 | 53.7 |
| Wait return → API return | 8.9 | 30.2 |

The traced all-call API p99 is **8.925 / 10.249 ms**; sampled harness p99 is **8.524 / 10.536 ms**. They use different populations and timestamp scopes and need not be identical. Stage percentiles do not sum to an API percentile; only each individual path and corresponding mean decomposition do.

In the slowest 1% of calls, submission-to-reap is the largest interval for **122 of 200** single-writer calls and **482 of 1,000** eight-writer calls. Single-writer extent preparation dominates another 73 tail calls; at eight writers, sync and FRONTIER writes dominate 188 and 160. These counts locate observed delays; they do not assign every long interval to SSD firmware or hardware.

The worker is frequently sampled sleeping in `do_sys_poll`, while the packer/coordinator also exhibit I/O and channel waits. Sampling cannot prove that every completion had already occurred or that the worker was runnable throughout a particular gap. A later optimization should therefore test completion delivery as a hypothesis rather than infer a device-only or notification-only cause.

## Validation and restoration

Temporary tracing passes **1,973 full tests**, focused worker/runtime tests, formatting, strict Clippy, dependency/spelling checks and the release build outside the sandbox. The full check has one slow test and no retries/skips. Captures produce 280,000 and 924,453 trace events, with zero drops.

The [instrumentation patch](artifacts/rfc-025-v7-point-handoff-20261029-instrumentation.patch) changes `wal.rs`, the parallel runtime/worker and `write-perf.rs`. All 128 baseline input hashes and the canonical executable are restored before the separately planned notification candidate. The [restoration record](artifacts/rfc-025-v7-point-handoff-20261029-restoration.json) describes that historical diagnostic-end state; the successor verifier owns final live-source checks.

| Executable | SHA-256 |
| --- | --- |
| Retained reference | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Instrumented diagnostic | `dbc0577ddf71b618f67ee2621af3ffceae7d445159a6bc6962c406e50fa73cb6` |

These two excluded invocations advance the lineage from 803 to **805**, plus the earlier interrupted invocation without JSON/run ID. Their IDs are counted once when the follow-on trial advances the total again. Native/event/sample files, per-call reconstructed paths and binaries remain in ignored local `target/` paths; versioned metadata binds their hashes. Disposable databases are removed.

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-point-handoff-20261029-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-point-handoff-20261029-verify.py
```

The [summary](artifacts/rfc-025-v7-point-handoff-20261029-summary.json), [capture identities](artifacts/rfc-025-v7-point-handoff-20261029-captures.json), [checks](artifacts/rfc-025-v7-point-handoff-20261029-checks.json) and [verification receipt](artifacts/rfc-025-v7-point-handoff-20261029-verification.json) reproduce the diagnosis. No traced throughput result contributes to candidate qualification.
