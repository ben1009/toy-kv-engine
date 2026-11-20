# RFC 026 V7/V8 DATA-completion timeline on GCP — 2026-11-19

## Scope and method

This diagnostic follows the [V7/V8 sync-decision snapshots](rfc-026-v7-v8-sync-decision-snapshots-gcp-20261119.md). It records the V8 DATA completion lifecycle from worker submit through userspace CQE observation, completion-reaper receive, `data_complete` publication, prefix advancement, and durability. It changes no WAL protocol or scheduling policy.

The source commit was `a7896495d4eed7db227d9d44cf7fb7d57af1fddd`; the release `write-perf` binary SHA-256 was `53a592a471c90ba18ffb336b199dbca49ee0720feeb3eaf08eb066b4e40f254c`. The host was a `c4-standard-16-lssd` with two NVMe Local SSDs in RAID0, ext4, and `nightly-2026-09-23`.

The suite ran 100,000 1 KiB Point puts per cell, with compaction disabled. It compared V7 Parallel with V8 Parallel using 1 MiB Point-only Lookahead, Direct FRONTIER, and dynamic coalescing. There was one diagnostic run per version at 1, 4, 8, and 16 writers (8 runs total). The completion timeline sampled one request per 128 puts on V8; each sample carries its worker group's ticket range. V7 has no corresponding timeline in this suite, so the detailed completion-stage timings below describe V8 only.

These are instrumented diagnostic runs, not throughput scores. Completion samples include clock reads, a coordinator-state inspection, and lock timing; those operations can perturb the sampled path. The lock-hold endpoint is captured immediately before the sample record is stored and before the mutex guard drops, so the reported hold duration omits that final bookkeeping and is a lower-bound estimate. There were 782 samples per V8 run and zero dropped timeline samples.

`submit → CQE observed` means the userspace worker observed the completion. It includes kernel completion, notification/polling, and scheduling delay; it is not a disk-only latency. The HOL blocker age starts when the producer begins publishing DATA to the ordered worker submitter, before SQE submission, so it includes queueing. It is not a kernel I/O duration.

## Sync-decision cross-check

The values below are mean contiguous ticket counts at the sync decision. The DATA prefix is the number of tickets whose DATA I/O had completed; `Frames` is the contiguous producer-frame-ready prefix.

| Writers | WAL | Syncs | Puts/sync | Admitted | Frames ready | DATA complete | Frames − DATA |
|---:|---|---:|---:|---:|---:|---:|---:|
| 8 | V7 | 12,981 | 7.70 | 7.76 | 7.76 | 7.70 | 0.06 |
| 8 | V8 | 13,115 | 7.62 | 7.78 | 7.78 | 7.62 | 0.16 |
| 16 | V7 | 8,296 | 12.05 | 13.34 | 13.34 | 12.05 | 1.29 |
| 16 | V8 | 10,167 | 9.84 | 12.15 | 12.00 | 9.84 | 2.16 |

At 8 writers the DATA-complete prefix is nearly the same as V7. At 16 writers V8's mean frame-ready to DATA-complete gap is 2.16 tickets, compared with 1.29 for V7; the V8 DATA-complete prefix p50 was 10 tickets versus 15 for V7. This reproduces the readiness gap in the earlier sync-decision run, but one diagnostic pass does not establish why it appears or how much it causes the throughput difference.

The diagnostic run's throughput was 32.55k puts/s for V7 and 29.17k for V8 at 8 writers, and 47.91k versus 40.47k at 16 writers. Process CPU was 40.79/56.67 μs per Put at 8 writers and 61.32/84.71 μs at 16 writers. These single-run, instrumented values are context only, not qualification scores.

## V8 DATA completion stages

Durations are p50 / p95 / p99 in microseconds across 782 sampled V8 group completions per writer count.

| V8 stage | 8 writers | 16 writers |
|---|---:|---:|
| DATA submit → CQE observed | 38.95 / 54.82 / 66.73 | 39.07 / 61.18 / 92.80 |
| CQE observed → group finished | 0.40 / 1.33 / 5.10 | 0.65 / 2.76 / 10.16 |
| Group finished → result send starts | 1.36 / 2.33 / 6.02 | 1.62 / 4.72 / 13.95 |
| Result send starts → reaper receives | 11.29 / 23.16 / 45.16 | 10.41 / 23.47 / 65.00 |
| Reaper receives → `data_complete` publication | 1.48 / 6.63 / 18.93 | 1.98 / 9.72 / 39.77 |
| `data_complete` lock wait, wall | 0.54 / 2.71 / 14.91 | 0.68 / 5.05 / 38.76 |
| `data_complete` lock hold, wall | 1.01 / 3.88 / 6.60 | 1.37 / 6.82 / 8.94 |
| Reaper receives → contiguous prefix ready | 1.66 / 7.87 / 19.98 | 2.27 / 11.25 / 39.98 |
| Prefix ready → durable | 102.30 / 250.27 / 482.10 | 158.80 / 401.23 / 567.84 |

At 16 writers the largest measured userspace interval after CQE observation is result delivery to reaper receive: 10.41 μs p50 and 65.00 μs p99. The coordinator lock wait median is 0.68 μs, while its p99 reaches 38.76 μs. This makes completion delivery/reaper scheduling a more specific follow-up than assuming the coordinator mutex is the main delay. The lock figures include sampled instrumentation and should be read as diagnostic estimates.

The first incomplete ticket was recorded after each sampled group completion. At 16 writers, 726 of 782 samples had such a blocker; 719 blockers were already frame-ready and 722 had their digest folded. The measured blocker DATA-write age was 60.04 μs p50, 112.33 μs p95, and 231.61 μs p99 (719 samples with a start timestamp). This confirms that an incomplete DATA ticket often already had producer-side readiness, but its age includes ordered-submitter queueing before SQE submission. The same condition also appeared at 8 writers (662 of 782 samples), where sync decisions showed almost no mean frame/DATA prefix gap; blocker presence alone therefore does not prove that it shortened the selected sync group.

`Prefix ready → durable` spans later driver work and durability, including coalescing and I/O. It is not a pure wait measurement. Stage percentiles are independent distributions and must not be summed into an end-to-end percentile.

## Interpretation and next diagnostic

The sync-decision gap is again visible at 16 writers, not 8. Within the V8 timeline, the CQE-to-group-finished interval and reaper-to-publication interval are short at the median; result-send-to-reaper-receive is longer, while lock wait is low at the median. At the same time, submit-to-CQE remains about 39 μs p50 and has a higher p95/p99 at 16 writers. Because this interval lacks a kernel completion timestamp, the data cannot separate device service time from completion notification and scheduling.

The evidence narrows the next measurement to two boundaries: obtain a kernel completion timestamp for the same DATA operations, and break down the worker-result handoff to the completion reaper (queue residence, wakeup, and reaper scheduling). The current run does not include a V7-matched timeline, uses one run per cell, and is instrumented; it cannot establish causality or justify changing Group Commit, notification, or WAL ordering.

## Validation, artifacts, and cleanup

The runner reported `complete: true` with 8/8 planned runs completed; `remote-exit.json` reported `failed: false` and 8 runs started. Each scheduled writer/version cell has a successful `result.json` and matching `run-metadata.json`. Every V8 run has a non-empty `completion-timeline.json` and 782 sampled records. The transferred archive SHA-256 is `aff7876191e8be499712b0d0548174971ede081ea963ddde74b2f9433cc41f7d`; it matches the retained collection metadata.

The raw archive and results remain local under `bench-results/gcp-pr558-data-completion-a7896495/` and are not checked in. After verification, the runner deleted the VM and temporary SSH rule. Terraform destroyed the isolated VPC, subnet, and egress route; read-only inventory checks confirmed all were absent, and the Terraform state contains zero resources. Billing, APIs, and quotas were not changed. The Free Trial estimate guard was $1.10 for a 60-minute lifetime with a $2 ceiling; this is an estimate, not a billing cap.
