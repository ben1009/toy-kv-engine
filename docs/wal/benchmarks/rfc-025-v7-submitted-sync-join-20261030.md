# RFC 025: submitted-follower durability join

## Result

This experiment adds a short v7 durability join after the existing coalescing
policy. It reduced syncs in the best eligible 8-writer candidate run, but that
run had worse p99 than the best eligible same-screen baseline. Most 16-writer
samples were stalled. The experiment does not meet the performance retention
criteria, so its source changes have been restored; the patch and all
measurements remain available below.

The existing coordinator already waits up to 100–800 µs for eligible admitted
work. Its 800 µs large-batch policy requires each completion to contain a
single large ticket; a multi-ticket group disables that classification. This
is a possible interaction with dynamic DATA grouping, not a proven cause of
the observed sync counts.

## Candidate

The candidate publishes an exclusive submitted-ticket hint after a v7 DATA
group is handed to the worker. After the existing coalescer, the coordinator
can wait for up to 10 µs for a submitted successor, provided admission is open,
the written prefix is physically eligible, and no poison is present. The
hint can only skip or request an optional wait: it never advances the written
or durable prefix. A lone writer has no submitted successor and takes no
intentional join wait. Scheduling can make the actual delay exceed the timer.

DATA grouping, the FRONTIER wire format, and the sync/acknowledgment protocol
are unchanged. The [candidate patch](artifacts/rfc-025-v7-submitted-sync-join-20261030-candidate.patch.gz)
is gzip-compressed and includes the submitted hint and tests; the current WAL source is restored to
the dynamic-window implementation at `0b87e88a`.

## Measurements

Each SSD run writes 750,000 1-KiB puts in batch64 on ext4, with Parallel v7
WAL and compaction disabled. The first screen ran three scored alternating
rounds plus warmups at 8, 16, and 1 writer. A separately planned follow-up ran
two scored alternating rounds plus warmups at 8 and 16 writers. A run was
flagged when mean frontier write time was at least 0.12 ms or mean `fdatasync`
time was at least 0.6 ms. These mean thresholds can miss shorter stalls.

The table selects the highest eligible throughput for each exact binary and
workload, including earlier 750,000-put runs of the same baseline binary.
Each p99 and counter is from that run. The 1- and 16-writer baseline peaks and
the 16-writer candidate peak came from different cohorts, so the comparison is
descriptive rather than a controlled attribution to the source change.

| Writers | Version | Puts/s | p99 (ms) | Syncs | DATA groups | Syncs/group | Syncs/buffer |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Dynamic baseline, earlier cohort | 83,417 | 1.200 | 11,719 | 11,719 | 1.000 | 1.000 |
| 1 | Submitted join | 79,230 | 1.197 | 11,719 | 11,719 | 1.000 | 1.000 |
| 8 | Dynamic baseline | 348,734 | 2.726 | 2,350 | 5,983 | 0.393 | 0.201 |
| 8 | Submitted join | 396,500 | 3.075 | 1,906 | 5,752 | 0.331 | 0.163 |
| 16 | Dynamic baseline, earlier cohort | 517,761 | 5.857 | 1,418 | 5,579 | 0.254 | 0.121 |
| 16 | Submitted join | 536,893 | 3.883 | 1,423 | 5,686 | 0.250 | 0.121 |

FRONTIER counts equaled sync counts in these selected runs. At eight writers
the best candidate had 19% fewer syncs and 14% more throughput, but p99 rose
13%. In the adjacent eligible pair, the candidate used 2,202 syncs versus
2,350, achieved 341,709 versus 348,734 puts/s, and had 2.722 versus 2.726 ms
p99. The follow-up had no eligible 8-writer measurements. Across the new
cohorts only one of five baseline and two of five candidate 8-writer runs
passed the filter.

At sixteen writers, only one of five new runs per arm passed the filter. The
baseline's eligible follow-up run nevertheless had 13.024 ms p99 and is not a
clean speed reference. The best candidate and earlier same-binary baseline had
almost identical syncs per buffer, despite the higher candidate throughput.
At one writer, the best current baseline was flagged, so the table uses the
previous best of that exact binary; both selected runs had zero coalescing wait
calls and one sync per batch. Differences across these cohorts may reflect
storage or host variation.

For context, historical v4 Parallel bests of the same workload were 85,618,
388,372, and 595,941 puts/s at 1, 8, and 16 writers, with p99 of 1.195, 2.511,
and 2.967 ms respectively. They were not paired with this experiment.

## Validation and artifacts

The candidate passed 97 WAL coordinator and worker tests outside the sandbox,
without retries. The added test exercises an unsubmitted follower, an
eligible submitted follower, timeout, close and poison checks, and recovery
after syncing the joined prefix. An existing test also checks that an
ineligible physical prefix is not joined. Point-workload guards were not run:
the predeclared batch screen did not qualify the candidate.

The [first plan](artifacts/rfc-025-v7-submitted-sync-join-20261030-plan.json),
[matrix](artifacts/rfc-025-v7-submitted-sync-join-20261030-matrix.json), and
[summary](artifacts/rfc-025-v7-submitted-sync-join-20261030-summary.json)
preserve all flags, counters, and run IDs. The separately planned
[follow-up](artifacts/rfc-025-v7-submitted-sync-join-retry-20261030-plan.json)
and its [summary](artifacts/rfc-025-v7-submitted-sync-join-retry-20261030-summary.json)
retain its stalled and eligible samples. Raw JSONL, host audits, run manifests,
capture logs, binary identities, and the
[environment record](artifacts/rfc-025-v7-submitted-sync-join-20261030-environment.json)
share those prefixes. Both analyzers revalidated all 36 invocations and their
artifact hashes after capture.
