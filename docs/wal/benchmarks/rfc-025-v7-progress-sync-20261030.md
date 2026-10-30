# RFC 025: short submitted-successor durability join

## Result

This experiment replaces v7's existing 100–800 µs admission-based coalescing
policy with a single 40 µs deadline, entered only when a successor has already
been submitted to the worker. It does not qualify for retention: every
high-concurrency candidate sample crossed the predeclared mean FRONTIER-write
threshold, so throughput and p99 comparisons remain inconclusive. The WAL
source was restored to the dynamic-grouping baseline; the patch and all
captures are preserved.

The flagged samples show the expected trade: less recorded coalescing wait
and more syncs. They do not establish that the policy is faster or slower.
The threshold is an aggregate signal, not proof that a particular run was
affected by a storage stall; it can also miss short stalls.

## Candidate

The ordered packer publishes an exclusive submitted-ticket end after the
worker accepts each v7 DATA group. The coordinator waits for completions only
when that hint is beyond an eligible written prefix, admission remains open,
and no poison is present. It rechecks progress after each completion but uses
one fixed deadline. A stale hint can only skip an optional wait; it cannot
advance the written or durable prefix. Scheduler delays can make a timed wait
take longer than its nominal 40 µs budget.

This is a submitted-successor first version, not an estimator of recent
completion rate or device pressure. It adds no second join after the old v7
policy: it replaces that policy. V4 retains its previous coalescing behavior.
DATA grouping, producer framing, SHA/I/O overlap, FRONTIER format, and
sync/acknowledgment rules are unchanged. The
[compressed candidate patch](artifacts/rfc-025-v7-progress-sync-20261030-candidate.patch.gz)
includes code and tests; current WAL source is restored to `7c55d461`.

## Measurements

Each ext4 SSD run writes 750,000 1-KiB puts in batch64 using Parallel v7 WAL
and no compaction. The initial screen's 8-writer cell completed, but the next
cell stopped before its first warmup because an unrelated Rust build was
detected. Its capture log and partial summary are retained. A separately
planned continuation ran 16, 1, and 8 writers with one warmup and three scored
alternating rounds per arm. Since the earlier high-concurrency candidate runs
were all flagged, a separate follow-up ran two scored alternating rounds per
arm at 8 and 16 writers after warmup. No scored runs were replaced.

Runs were flagged when mean FRONTIER write time was at least 0.12 ms or mean
`fdatasync` time was at least 0.6 ms. Across all new high-concurrency captures,
the candidate had zero eligible samples: 0/8 at eight writers and 0/5 at
sixteen. At one writer, each arm had one eligible sample out of three.

The table shows the highest eligible baseline and the highest raw candidate
within the named cohort; the two candidate high-concurrency rows are flagged.
Each p99 and counter belongs to the same invocation as its throughput.

| Writers | Version | Puts/s | p99 (ms) | Syncs | DATA groups | Syncs/group | Wait calls | Wait time (ms) | Flag |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | Dynamic baseline | 84,491 | 1.203 | 11,719 | 11,719 | 1.000 | 0 | 0.0 | No |
| 1 | 40 µs submitted join | 82,719 | 1.211 | 11,719 | 11,719 | 1.000 | 0 | 0.0 | No |
| 8 | Dynamic baseline, follow-up | 330,316 | 2.860 | 2,408 | 6,022 | 0.400 | 3,858 | 639.8 | No |
| 8 | 40 µs submitted join, follow-up | 318,830 | 2.823 | 3,627 | 6,256 | 0.580 | 1,444 | 117.2 | Yes |
| 16 | Dynamic baseline, continuation | 544,020 | 5.097 | 1,420 | 5,645 | 0.252 | 3,541 | 500.9 | No |
| 16 | 40 µs submitted join, continuation | 562,895 | 3.385 | 1,929 | 5,400 | 0.357 | 1,063 | 99.8 | Yes |

The 8-writer candidate's mean FRONTIER write was 0.173 ms, and the 16-writer
candidate's was 0.164 ms. Their p99 values do not by themselves show a large
latency spike, but both fail the frozen 0.12 ms mean threshold. No candidate
performance win is inferred from them. FRONTIER counts matched sync counts in
all selected samples; the one-writer samples both performed one sync per batch
and recorded no coalescing waits.

For the flagged 8-writer pair, `saved_syncs` is **−1,219** and added
coalescing wait is **−522,550 µs**: the candidate saved wait but performed
about **2.33 additional syncs per millisecond of wait saved**. The analogous
16-writer comparison is **−509** saved syncs and **−401,090 µs** added wait,
or about **1.27 additional syncs per millisecond saved**. These are descriptive
sample ratios, not causal estimates. At eight writers, measured `fdatasync`
time rose from 839.2 to 1,255.8 ms and FRONTIER-write time from 190.1 to
627.0 ms. These intervals overlap with work on other threads and are not an
additive decomposition of elapsed time.

For the highest-score context, the best eligible earlier or current result of
the same baseline binary was 387,287 puts/s at 8 writers and 567,320 puts/s at
16 writers. The candidate's corresponding best raw results above were flagged
and came from different cohorts. Historical v4 Parallel best throughput for
the same workload was 85,618, 388,372, and 595,941 puts/s at 1, 8, and 16
writers respectively; these runs were not paired with the candidate.

## Validation and artifacts

The candidate passed 96 WAL coordinator and worker tests outside the sandbox
without retries. The tests cover unsubmitted work, bounded waiting, eligible
and ineligible prefixes, poison and close checks, out-of-order completion,
and recovery after the joined prefix. Point-workload guards were not run
because the batch screen did not qualify the candidate.

The [initial plan](artifacts/rfc-025-v7-progress-sync-20261030-plan.json),
[partial summary](artifacts/rfc-025-v7-progress-sync-20261030-partial-summary.json),
and [interruption log](artifacts/rfc-025-v7-progress-sync-20261030-capture.log)
preserve the first cell and the reason the next one stopped. The separately
planned [continuation](artifacts/rfc-025-v7-progress-sync-continuation-20261030-plan.json)
and [summary](artifacts/rfc-025-v7-progress-sync-continuation-20261030-summary.json),
plus the [follow-up](artifacts/rfc-025-v7-progress-sync-followup-20261030-plan.json)
and [summary](artifacts/rfc-025-v7-progress-sync-followup-20261030-summary.json),
retain all samples and flags. Raw JSONL, host audits, run manifests, capture
logs, binary identities, and the
[environment record](artifacts/rfc-025-v7-progress-sync-20261030-environment.json)
share those prefixes. The analyzers revalidated 44 unique invocations and their
artifact hashes after capture.
