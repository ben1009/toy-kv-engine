# V7/V8 Point DATA submit and head-of-line timeline rerun on GCP

Run date: 2026-11-19 UTC
Source commit: `b02c287e02d533b2e370d8a6a9c9ad05367f3c7b`

## Summary

This rerun adds matching V7/V8 timestamps around Point DATA submission, worker
queue acceptance, SQE submission, CQE observation, completion handling, and
publication of `data_complete`. It also samples the V8 ticket currently
blocking the contiguous DATA-complete prefix.

The observed queue-to-SQE and submit-to-CQE medians do not show a clear V8
penalty. At 16 writers, V8's median send-to-reaper delay is lower than V7's;
V8's reaper-receive-to-`data_complete` median is higher by about 1.5 μs. The
more persistent difference is the shorter V8 DATA-complete prefix at sync
decisions: 9.95 tickets on average versus 12.16 for V7. These data narrow the
next investigation, but one instrumented run per cell cannot establish the
cause of the throughput difference.

## Setup and verification

- Machine: GCP `c4-standard-16-lssd`, 16 vCPUs, two 375 GiB Local SSDs, ext4
  RAID0.
- Workload: 100,000 Point puts per run, 1 KiB values, 1, 4, 8, and 16 writers.
- V7: Parallel WAL. V8: Parallel WAL with 1 MiB Point-only Lookahead, Direct
  FRONTIER, dynamic coalescing, and the existing `io_uring` DATA path.
- This is a diagnostic-only suite: one run for each of the 8 version/writer
  cells. Producer CPU attribution sampled every 64 operations; completion
  timelines sampled every 128 events (782 events per cell). Do not treat the
  throughput or latency values below as uninstrumented benchmark scores.
- The earlier attempt stopped after 1 of 8 runs because the runner rejected
  omitted optional HOL fields. The parser fix is included in the source commit
  above. A fresh VM rerun completed all 8 cells.
- `summary.json` reports `complete: true`, `results_complete: true`, and 8/8
  runs. `remote-exit.json` reports `failed: false` and 8 runs started. The
  matrix validator reports no missing, duplicate, or invalid cells. Every run
  has a successful `result.json` and matching `run-metadata.json`.
- Source binary SHA-256:
  `53c73d2d17f7550e7a26d02d84c9bd2cad4dedffdc0d85bf34fd6167ac75cd7b`.
- GCP runner SHA-256:
  `cc5624af2c7d98f2b09b6d28c75fd69feb92ad0d5330213c28c707f625e17146`.
- Verified results archive SHA-256:
  `086df36a1efaecae178ff8562a23c6c3dc5f8023c544140aedbfcab1a861a7e4`.
  The raw archive remains local at
  `/tmp/gcp-v8-point-cpu-attribution-b02c287e-20261120-run2/collection-attempt-01/results.tar.gz`;
  it is not committed to the repository.
- After collection and checksum verification, the VM, its boot disk, and the
  temporary SSH firewall were verified absent. The pre-existing benchmark VPC
  and subnet were retained. Billing, APIs, and quota settings were not changed.

## Instrumented run context

Each cell ran once. CPU/Put is process user plus system CPU divided by 100,000
operations. Puts/sync is calculated from the observed sync count.

| Writers | WAL | Throughput (k puts/s) | Process CPU/Put (μs) | Syncs | Puts/sync |
|---:|---|---:|---:|---:|---:|
| 1 | V7 | 6.42 | 74.59 | 100,000 | 1.00 |
| 1 | V8 | 7.32 | 63.15 | 100,000 | 1.00 |
| 4 | V7 | 21.27 | 39.45 | 25,068 | 3.99 |
| 4 | V8 | 19.89 | 46.53 | 25,213 | 3.97 |
| 8 | V7 | 34.51 | 40.37 | 12,965 | 7.71 |
| 8 | V8 | 27.60 | 56.82 | 13,100 | 7.63 |
| 16 | V7 | 47.71 | 59.84 | 8,222 | 12.16 |
| 16 | V8 | 41.07 | 81.07 | 10,046 | 9.95 |

At 16 writers, this diagnostic run shows V8 at 13.9% lower throughput and
21.23 μs higher process CPU per put. This is a single instrumented observation,
not a paired estimate or a stable performance score. The 1-writer result also
shows why the data should not be generalized across concurrency levels.

## DATA completion prefixes at durability decisions

These are mean contiguous prefix lengths from the sync-decision observations.

| Writers | WAL | Admitted | Frame-ready | DATA-complete |
|---:|---|---:|---:|---:|
| 8 | V7 | 7.77 | 7.77 | 7.71 |
| 8 | V8 | 7.80 | 7.80 | 7.63 |
| 16 | V7 | 13.42 | 13.42 | 12.16 |
| 16 | V8 | 12.24 | 12.09 | 9.95 |

At 8 writers, the mean DATA-complete prefixes are close. At 16 writers, V8
enters the sync decision with a shorter admitted prefix and a larger gap between
frame readiness and DATA completion. This is consistent with the previous
sync-decision report; it does not by itself identify whether the difference is
caused by the producer cadence, submitter queue, I/O completion, or scheduling.

## Matched DATA completion timeline

Values are p50 / p95 in μs across 782 sampled events per cell. V7 and V8 use
corresponding handoff boundaries. Do not add the independent percentile values
to estimate an end-to-end percentile.

| Stage | 8w V7 | 8w V8 | 16w V7 | 16w V8 |
|---|---:|---:|---:|---:|
| Frame ready → submit request | 1.85 / 7.49 | 1.47 / 7.82 | 2.22 / 11.21 | 1.84 / 20.57 |
| Submit request → worker queue | 0.06 / 0.31 | 0.12 / 0.27 | 0.08 / 0.40 | 0.21 / 0.41 |
| Worker queue → worker accept | 7.83 / 22.69 | 5.06 / 20.07 | 6.35 / 23.51 | 5.27 / 20.33 |
| Worker accept → SQE submit | 6.17 / 21.50 | 4.28 / 16.48 | 7.34 / 24.16 | 5.08 / 21.31 |
| SQE submit → CQE observed | 42.23 / 56.66 | 38.70 / 59.20 | 41.85 / 63.40 | 38.88 / 64.60 |
| CQE observed → group finished | 0.60 / 4.39 | 0.45 / 1.38 | 0.91 / 4.83 | 0.69 / 3.64 |
| Group finished → result send | 1.24 / 2.17 | 1.53 / 3.07 | 1.47 / 3.14 | 1.71 / 5.64 |
| Result send → receiver/reaper | 11.51 / 19.23 | 10.79 / 22.28 | 11.88 / 73.17 | 9.20 / 24.01 |
| Receiver/reaper → `data_complete` publish | 0.49 / 1.28 | 1.56 / 8.05 | 0.60 / 3.19 | 2.14 / 10.76 |

The measured worker-queue and worker-accept-to-SQE medians are not higher for
V8. Submit-to-CQE-observation medians are also similar or lower for V8; p95 is
similar. This interval ends when userspace observes the CQE and includes
completion notification and scheduling, so it is not an SSD-only latency.

At 16 writers, V8's result-send-to-reaper median is 9.20 μs versus 11.88 μs for
V7. V8's reaper-to-publication median is 2.14 μs versus 0.60 μs for V7, with
p95 10.76 versus 3.19 μs. These values make post-receive processing worth
watching, but do not explain the full prefix or throughput difference on their
own. V7 also has a longer p99 in the receiver-to-publication interval in this
single run.

## V8 head-of-line observations

At 16 writers, a blocker was present in 723/782 sampled completion events.
Among those blocker observations, 709 reported the blocker frame-ready and 715
reported its digest folded. The blocker DATA-write age had p50/p95/p99 of
61.52/128.22/278.15 μs across 708 samples.

This age begins when the producer starts publishing DATA to the ordered worker
submitter and includes queueing. It is not a measurement of pure waiting before
SQE submission. The matched timeline above separates queue acceptance and SQE
submission for each sampled event, but those event phases are not all directly
keyed to the same blocker ticket. The current data therefore does not prove
whether the relevant HOL delay is before SQE submission or after it.

## Producer CPU context

Sampled Thread CPU p50, in μs per sampled Point operation:

| Writers | WAL | Producer total | Sequencer wait | Admission | Frame finish |
|---:|---|---:|---:|---:|---:|
| 8 | V7 | 7.04 | 0.31 | 1.92 | — |
| 8 | V8 | 21.31 | 3.05 | 2.71 | 8.64 |
| 16 | V7 | 8.80 | 0.36 | 2.31 | — |
| 16 | V8 | 28.01 | 3.35 | 2.98 | 9.92 |

These are sampled stage medians with different V7/V8 stage availability;
subphases overlap the enclosing producer and frame-finish intervals and must
not be added. They keep Producer and Frame Finish as plausible follow-up areas,
not proven causes.

## Conclusion

This rerun weakens the hypothesis that V8 is slower because its ordered worker
queue or kernel-completion observation is universally slower: the measured
medians are similar or lower for V8. It confirms that V8's DATA-complete prefix
is shorter at 16-writer sync decisions, while 8-writer prefix lengths are
similar. V8 has a modestly longer reaper-to-publication median and tail in this
sample, but V7 also has a larger p99 outlier in the corresponding interval.

The next diagnostic should correlate the blocker ticket itself with its frame
ready, worker queue, SQE submit, CQE observation, and `data_complete` publication
timestamps, rather than infer its delay from independent phase percentiles.
Repeat only the 8- and 16-writer diagnostic cells if a repeated trace is needed;
do not change the ordered submitter or completion architecture based on this
single pass.
