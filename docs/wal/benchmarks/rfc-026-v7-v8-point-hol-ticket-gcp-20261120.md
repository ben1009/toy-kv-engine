# V7/V8 Point same-ticket DATA HOL diagnosis on GCP

Run date: 2026-11-20 UTC  
Source commit: `bba654ff82da7596bcdedda18295c01355ba1b90`

## Summary

This diagnostic joins each sampled sync decision to the exact first Ticket
outside its contiguous DATA-complete prefix. It distinguishes a blocker that is
not frame-ready, waiting for SQE submission, submitted but awaiting CQE
observation, or CQE-observed but not yet published as `data_complete`.

At 16 writers, V8 still reaches sync decisions with a shorter DATA-complete
prefix than V7 (9.83 versus 12.05 Tickets on average). In the sampled,
same-ticket blocker joins, most blockers in both versions had already reached
SQE submission; V8 had more sampled decisions where CQE observation preceded
`data_complete` publication. The generic submit-to-CQE and worker-send-to-reaper
medians were not slower for V8. This makes completion publication worth a
focused follow-up, but does not establish that it causes the throughput gap.

## Setup and artifact verification

- Machine: GCP C4 16-vCPU host, two Local SSDs in ext4 RAID0.
- Workload: 100,000 Point puts, 1 KiB values, 8 and 16 writers.
- Arms: V7 Parallel and V8 Parallel with 1 MiB Point-only Lookahead, Direct
  FRONTIER, dynamic coalescing, and the existing `io_uring` DATA path.
- One instrumented run per version/writer cell (4 runs total). Producer CPU
  attribution sampled every 64 operations; DATA completion events and eligible
  same-ticket HOL blockers sampled every 32 Tickets.
- These are diagnostics, not uninstrumented throughput scores. There is one
  observation per cell, and detailed instrumentation can affect scheduling.
- `summary.json` reports `complete: true` and 4/4 runs. `remote-exit.json`
  reports `failed: false` and 4 runs started. Every planned run has a successful
  `result.json` and matching `run-metadata.json`; sampled timeline drops are
  zero and all cadence-eligible blockers join to a Ticket timeline.
- Source binary SHA-256:
  `a6e7ca1de1c87bfa9800d30c07b694947306ce323dd65a109b087fe24e5e7679`.
- The verified results archive remains local, not in the repository:
  `/tmp/gcp-v8-hol-ticket-bba654ff-20261120-run-b-retry2/recovery-01/collection-attempt-01/results.tar.gz`.
  SHA-256: `93b4494fc6db910fd1e90e9b6a6498c3ad15bfb81819ac8aa4ec4e25a8180142`.
- The benchmark VM and boot disk were deleted and verified absent; its
  temporary SSH firewall rule is absent. The reusable shared network was
  pre-existing, had no attached instances, and was left unchanged.

## Instrumented run context

Process CPU is user plus system CPU divided by 100,000 operations. Puts/sync is
calculated from the observed sync count. Treat throughput and CPU values here
as diagnostic context only.

| Writers | WAL | Throughput (k puts/s) | Process CPU/Put (μs) | Syncs | Puts/sync |
|---:|---|---:|---:|---:|---:|
| 8 | V7 Parallel | 29.97 | 41.32 | 13,070 | 7.65 |
| 8 | V8 Lookahead + Direct FRONTIER | 27.73 | 54.29 | 13,172 | 7.59 |
| 16 | V7 Parallel | 45.51 | 60.99 | 8,300 | 12.05 |
| 16 | V8 Lookahead + Direct FRONTIER | 38.19 | 84.48 | 10,174 | 9.83 |

At 8 writers, the mean admitted and frame-ready prefixes were nearly identical,
and the DATA-complete means differed by only 0.06 Tickets. At 16 writers, V8
had shorter means at each point in the prefix:

| 16-writer sync snapshot | V7 | V8 |
|---|---:|---:|
| Admitted prefix | 13.43 | 12.17 |
| Frame-ready prefix | 13.43 | 12.02 |
| DATA-complete prefix | 12.05 | 9.83 |

These snapshots localize where the prefix is shorter, but do not by themselves
show which stage limits end-to-end throughput.

## Matched completion stages

Values are p50 / p95 in μs over 3,125 sampled events per cell. Percentiles are
computed independently and must not be added to infer an end-to-end percentile.
`Submit → CQE observed` includes kernel completion, notification, and userspace
scheduling; it is not SSD-only latency.

| Stage | 8w V7 | 8w V8 | 16w V7 | 16w V8 |
|---|---:|---:|---:|---:|
| Submit → CQE observed | 42.57 / 111.63 | 41.46 / 95.07 | 42.22 / 97.16 | 40.19 / 86.82 |
| Worker result send → reaper receive | 11.66 / 18.92 | 11.96 / 24.28 | 11.64 / 73.19 | 9.54 / 24.34 |
| Reaper receive → `data_complete` publish | 0.48 / 1.32 | 1.48 / 7.76 | 0.56 / 2.66 | 1.99 / 10.60 |

V8's median `data_complete` publication interval is higher in both cells, while
the preceding handoff medians are similar and submit-to-CQE is slightly lower.
The publication difference is only a few microseconds at p50; its larger p95
is a candidate for follow-up, not an explanation of the full V7/V8 gap.

## Same-ticket blocker stage at sync decision

The following are cadence-eligible, exact-ticket joins, not all blocker
observations. Counts are sample observations; they are not independent samples
or population percentages.

| Writers | WAL | Matched blockers | Not frame-ready | Frame-ready, pre-SQE | SQE submitted, pre-CQE | CQE observed, pre-publish |
|---:|---|---:|---:|---:|---:|---:|
| 8 | V7 | 16 | 2 | 6 | 7 | 1 |
| 8 | V8 | 19 | 0 | 0 | 11 | 8 |
| 16 | V7 | 78 | 4 | 4 | 61 | 9 |
| 16 | V8 | 221 | 2 | 9 | 137 | 73 |

At 16 writers, most matched blockers in both versions were already submitted
but had no observed CQE at the sync snapshot. V8 also had more matched samples
in the CQE-observed/pre-publication state (73 versus 9), alongside the longer
reaper-to-publication p50/p95. Because the sample counts and sync rates differ,
these counts do not quantify a causal contribution to throughput.

V8 completion timeline context:

| Writers | Blocker write age p50 / p95 | Reaper receive → prefix ready p50 / p95 | Prefix ready → durable p50 / p95 |
|---:|---:|---:|---:|
| 8 | 65.24 / 141.62 μs | 1.65 / 8.65 μs | 115.02 / 326.03 μs |
| 16 | 61.96 / 151.12 μs | 2.34 / 12.66 μs | 175.82 / 449.67 μs |

The blocker write age starts at the DATA submission handoff and includes
queueing; it is not pure kernel I/O time. Prefix-ready-to-durable includes
subsequent durability work, not just `fdatasync`.

## Conclusion and next step

The same-ticket trace does not support blaming the ordered submitter or CQE
observation alone: their V8 medians are not worse. It does show a repeatable
shorter V8 DATA-complete prefix at 16-writer sync decisions and a larger
CQE-observed/pre-publication count in the sampled blocker joins. The most
targeted next measurement is therefore the `data_complete` publication path,
including its lock wait/hold and scheduling interval, using matched V7/V8
boundaries and an uninstrumented control for throughput.

Do not change the ordered submitter, completion reaper, or Group Commit policy
based on this single diagnostic pass. The report narrows the next experiment;
it does not establish a runtime optimization target yet.
