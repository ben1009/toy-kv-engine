# RFC 025: completion-cadence durability join

## Result

The completion-cadence candidate did **not** qualify for retention. The v7 WAL
source has been restored to the dynamic DATA-grouping baseline. The candidate
passed correctness checks, but the one-writer comparison did not improve both
throughput and p99, and no high-concurrency candidate sample passed the
predeclared timing filter. The follow-up high-concurrency cohort also showed
large latency spikes in both arms.

The measurements do show that this policy made very few coalescing waits and
performed more syncs. They do not establish a high-concurrency performance win
or a causal explanation for why individual gate checks declined to wait.

## Candidate

The candidate replaces the v7 admission-based 100–800 µs coalescing policy.
It waits only when a written prefix is eligible for a frontier, an ordered
successor has been submitted to the worker, admission is open, and there is no
poison. It uses the last two **distinct worker-observed completion times**:
if their gap and the age of the latest observation are each below 50 µs, the
initial wait budget is `min(2 × gap, 50 µs)`. The coordinator reevaluates
after progress but does not extend the initial deadline. A late scheduled
wake can exceed the nominal deadline.

The worker timestamps groups when it drains their DATA CQEs; this is not a
hardware completion timestamp. Multiple groups completed in one drain share
one observation, so a queue of results from the same drain does not create
artificial zero gaps. The age check excludes an old burst after a long sync.
The submitted end is a hint only: it cannot advance the written or durable
prefix. V4 retains its existing coalescing policy. DATA grouping, producer
framing, ordered hashing after submission, the wire format, and durability
and acknowledgment rules are unchanged.

The [candidate patch](artifacts/rfc-025-v7-cadence-sync-20261030-candidate.patch.gz)
contains the implementation and tests. The initial source snapshot was
accidentally captured using the repository's human-readable external diff.
After the runs, the unchanged candidate was exported again as an applicable
patch, verified against that snapshot, and reversed. The initial
[rendered snapshot](artifacts/rfc-025-v7-cadence-sync-20261030-candidate-rendered-diff.gz)
is also preserved.

## Measurements

Each ext4 SSD run writes 750,000 1-KiB puts in batch64 using Parallel v7 WAL
and no compaction. The initial screen used one warmup and three scored,
alternating runs per arm at each writer count. A separately planned follow-up
used one warmup and two scored runs per arm at eight and sixteen writers.
The exact baseline and candidate binaries, host activity and every invocation
were recorded; 36 unique invocations were checked.

A run was flagged if mean FRONTIER-write time was at least 0.12 ms or mean
`fdatasync` time was at least 0.6 ms. These aggregate thresholds do not prove a
particular storage stall and can miss short stalls. Across the initial and
follow-up runs, baseline and candidate had respectively 1/5 and 0/5 eligible
scored runs at both eight and sixteen writers. At one writer, the baseline had
2/3 and the candidate had 1/3 eligible scored runs. Thus the minimum of two
eligible runs per arm was not met at any writer count.

The table shows each arm's highest eligible result in the initial screen,
except that the high-concurrency candidate rows show the highest **raw**
result because no eligible sample exists. Each p99 and counter is from the same
invocation as its throughput; flagged rows are diagnostics, not qualified
comparisons.

| Writers | Arm | Puts/s | p99 (ms) | Syncs | DATA groups | Syncs/group | Wait calls | Wait (ms) | Flagged |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | Dynamic baseline | 81,734 | 1.201 | 11,719 | 11,719 | 1.000 | 0 | 0.0 | No |
| 1 | Cadence | 82,956 | 1.213 | 11,719 | 11,719 | 1.000 | 0 | 0.0 | No |
| 8 | Dynamic baseline | 312,090 | 2.956 | 2,503 | 6,071 | 0.412 | 3,863 | 649.6 | No |
| 8 | Cadence | 314,984 | 2.742 | 3,720 | 6,466 | 0.575 | 33 | 2.6 | Yes |
| 16 | Dynamic baseline | 558,009 | 4.145 | 1,380 | 5,471 | 0.252 | 3,255 | 450.3 | No |
| 16 | Cadence | 539,061 | 3.754 | 1,995 | 5,470 | 0.365 | 100 | 8.9 | Yes |

The one-writer candidate was 1.5% faster with 1.0% higher p99, using the same
number of syncs and no coalescing waits. It did not satisfy the requirement
that both throughput and p99 improve, and it had only one eligible run.

In the flagged 8-writer comparison, the candidate had 1,217 more syncs and
647.0 ms less measured coalescing wait, or 1.88 additional syncs per
millisecond of wait saved. The corresponding 16-writer comparison is 615 more
syncs and 441.5 ms less wait, or 1.39 additional syncs per millisecond saved.
These are descriptive ratios between selected invocations, not causal
estimates. At eight writers the measured `fdatasync` totals were 891.2 and
1,284.6 ms, and FRONTIER-write totals were 197.7 and 717.0 ms. At sixteen
writers they were 514.4 and 744.3 ms for `fdatasync`, and 148.0 and 416.0 ms
for FRONTIER writes. Timings can overlap other threads and are not additive
components of elapsed time.

The 8-writer candidate's mean FRONTIER write was 0.193 ms and the 16-writer
candidate's was 0.209 ms; both crossed the preset 0.12 ms threshold even
though their p99 values were below the selected baseline samples. In the
follow-up, the best raw 8-writer runs were 83,498 and 80,399 puts/s, with
22.1 and 27.9 ms p99; at sixteen writers they were 377,144 and 344,140 puts/s,
with 15.8 and 15.9 ms p99. All four were flagged, and both arms had large
latency spikes; they are excluded from the table above.

For context, historical best eligible results for the same dynamic baseline
binary were 84,491, 387,287, and 567,320 puts/s at 1, 8, and 16 writers.
Historical best v4 Parallel results for the same workload were 85,618,
388,372, and 595,941 puts/s respectively. These are from different cohorts,
not paired runs; the candidate high-concurrency results above are flagged.

## Validation and artifacts

The candidate passed 99 WAL coordinator and worker tests outside the sandbox
without retries and passed a normal non-benchmark `cargo check`. The tests
exercise real worker timestamp capture, duplicate and stale observations,
eligible and ineligible physical prefixes, unsubmitted successors, poison,
close, out-of-order completion, and recovery after a joined prefix. Point
workload guards were not run because the batch screen did not qualify.

The [initial plan](artifacts/rfc-025-v7-cadence-sync-20261030-plan.json) and
[summary](artifacts/rfc-025-v7-cadence-sync-20261030-summary.json),
[follow-up plan](artifacts/rfc-025-v7-cadence-sync-followup-20261030-plan.json)
and [summary](artifacts/rfc-025-v7-cadence-sync-followup-20261030-summary.json),
and [environment record](artifacts/rfc-025-v7-cadence-sync-20261030-environment.json)
contain the analysis and identities. Raw JSONL, host audits, run manifests,
capture logs, and scripts share those prefixes.
