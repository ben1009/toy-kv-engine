# RFC 025: grouping admitted v7 tickets, 2026-10-29

**Performance remains inconclusive, so the candidate is not retained.** Producer framing now records a group entry as soon as a ticket is admitted, and a separate readiness queue controls submission. This reduces group fragmentation in the measured concurrent batches. The candidate's best one- and sixteen-writer batch64 samples improve over the fresh retained-v7 samples, but the SSD stalled in every scored eight-writer candidate sample across two cohorts. Point-write guards were not run because that batch screen could not be qualified.

The prior producer-framing and post-submit SHA candidate was restored as the starting point. For v7, admission records each ticket, DATA offset, length and commit timestamp in a separate FIFO, even while its producer still owns the frame job. Once the head is ready, the packer snapshots up to eight admitted tickets. If selected successors are unfinished, it waits on the frame-readiness condition for up to 100 microseconds, then submits the contiguous ready prefix. A lone writer or fully ready group has no intentional join wait. The packer checks the ready batches against their admission entries before submission. Small writes retain their original MVCC guard lifetime. The wire format, rolling SHA definition, FRONTIER and one-sync publication remain unchanged.

The join bound prevents an unfinished *later* producer from indefinitely holding an already-ready prefix. An unfinished head cannot be acknowledged out of order: if its job fails or is dropped, the runtime poisons that ticket; if it merely stops indefinitely, this candidate has no timeout or takeover mechanism. Close behavior and the decision to fail a stalled head require separate consideration before claiming a general bounded-progress guarantee.

## Initial SSD cohort

Each cell has one warmup per arm and three alternating scored rounds. The selected entry is the highest-throughput scored run for that exact binary whose mean FRONTIER write is below 1 ms and mean `fdatasync` is below 1.5 ms; p99 comes from that same run. The previous candidate is the immediate-drain prototype from the preceding report. Values are puts/s / p99 ms.

| Batch64 writers | Retained v7 | Previous candidate | Admitted-window candidate |
| --- | ---: | ---: | ---: |
| 1 | 70,778 / 1.262 | 80,224 / 1.194 | 80,469 / 1.203 |
| 8 | 395,842 / 3.093 | 335,833 / 2.246 | 313,789 / 8.702* |
| 16 | 500,057 / 4.595 | 483,138 / 4.197 | 610,513 / 3.832 |

The eight-writer candidate entry is marked because all three candidate samples have 8.7–9.0 ms p99 and 0.183–0.271 ms mean FRONTIER writes. They pass the broad fixed mask but clearly differ from the retained winner's 0.062 ms mean FRONTIER write. They do not establish a source-induced throughput regression. Several one- and sixteen-writer samples likewise show obvious stalls despite passing the broad mask. Averages do not identify every stall; all raw results are retained.

The selected eight-writer runs show the grouping effect:

| Binary | Groups | Solo groups | DATA buffers |
| --- | ---: | ---: | ---: |
| Retained v7 | 7,212 | 3,952 | 11,720 |
| Previous candidate | 11,348 | 11,088 | 11,720 |
| Admitted-window candidate | 9,703 | 7,957 | 11,720 |

In all three initial eight-writer candidate samples, the candidate formed 9,703–10,227 groups, versus 11,186–11,348 for the previous candidate. This is a real change in grouping, though it does not restore retained v7's 7,212–7,406 groups. At sixteen writers, the selected candidate formed 7,894 groups and 4,791 solo groups, versus 11,007 and 10,550 in the previous candidate's selected run.

## Longer-cooldown follow-up

Because every eight-writer candidate sample in the initial cohort showed both high p99 and elevated mean FRONTIER-write time, a distinct follow-up compared all three unchanged binaries. Its plan was frozen before capture: one warmup and three scored alternating rounds per arm, a two-minute initial cooldown, and one minute between every invocation. The original results were not replaced. This cohort independently flags mean FRONTIER writes at or above 0.12 ms or mean `fdatasync` at or above 0.6 ms for every arm; it requires at least two eligible scored runs per retained and candidate arm.

| Binary | Eligible scored runs | Mean FRONTIER write across the three scored runs | p99 across the three scored runs |
| --- | ---: | ---: | ---: |
| Retained v7 | 0 / 3 | 0.121–0.151 ms | 7.445–9.534 ms |
| Previous candidate | 1 / 3 | 0.109–0.244 ms | 7.446–8.475 ms |
| Admitted-window candidate | 0 / 3 | 0.138–0.248 ms | 7.630–9.513 ms |

The threshold still missed one previous-candidate run with an 8.475 ms p99. The follow-up is therefore inconclusive, not a clean regression estimate. Its three candidate runs formed 9,320–9,973 groups; the previous candidate formed 11,116–11,286. Point guards were conditional on a qualified batch screen and were not run.

## Historical highs

These are highest eligible scores for the exact retained executable from unequal historical cohorts; the candidate column is its initial fresh best. This is context rather than a paired comparison.

| Batch64 writers | v4 Parallel puts/s / p99 ms | Retained v7 historical best | Admitted-window candidate |
| --- | ---: | ---: | ---: |
| 1 | 85,618 / 1.195 | 72,647 / 1.373 | 80,469 / 1.203 |
| 8 | 388,372 / 2.511 | 478,798 / 2.653 | Not qualified |
| 16 | 595,941 / 2.967 | 606,948 / 4.065 | 610,513 / 3.832 |

The candidate's one-writer high is 6.0% below the v4 throughput high. At sixteen writers its throughput high is 2.4% above v4 and 0.6% above retained v7, while v4's selected p99 remains lower. These separate cohorts do not establish general v4 parity.

## Validation and artifacts

The candidate passed the full `cargo make check`: **1,986 tests**, retries disabled, including **66 focused WAL and guard tests**. Six admission, failure and close tests passed ten consecutive stress runs. A deterministic test selects a window containing unfinished successors, completes them out of order, verifies one three-buffer group, and checks recovery. Another test holds a successor unfinished and verifies the ready prefix can commit. These checks were run outside the sandbox with repository-local scratch.

The initial cohort contains **36 invocations** and the longer-cooldown follow-up **12**, including warmups. The initial cell cooldown was two minutes with 15-second gaps. Neither source nor binaries changed during either capture, and sampled audits detected no recognized competing builds, tests or benchmarks. The candidate differs from the previous candidate's source manifest only in the WAL runtime file. After the inconclusive result, the five experimental Rust files and the retained canonical binary were restored; the exact candidate binary and source patch remain available. The restored full check passed **1,973 tests**, with retries disabled.

The [initial plan](artifacts/rfc-025-v7-admitted-group-20261029-plan.json), [initial results](artifacts/rfc-025-v7-admitted-group-20261029-summary.json), [follow-up plan](artifacts/rfc-025-v7-admitted-group-cool-20261029-plan.json), [follow-up results](artifacts/rfc-025-v7-admitted-group-cool-20261029-summary.json), [candidate patch](artifacts/rfc-025-v7-admitted-group-20261029-candidate.patch), [checks](artifacts/rfc-025-v7-admitted-group-20261029-checks.json), [disposition](artifacts/rfc-025-v7-admitted-group-20261029-outcome.json), and [verification receipt](artifacts/rfc-025-v7-admitted-group-20261029-verification.json) preserve the experiment. Raw captures: [batch1](artifacts/rfc-025-v7-admitted-group-20261029-ext4-batch1.jsonl), [batch8](artifacts/rfc-025-v7-admitted-group-20261029-ext4-batch8.jsonl), [batch16](artifacts/rfc-025-v7-admitted-group-20261029-ext4-batch16.jsonl), and [batch8 with longer cooling](artifacts/rfc-025-v7-admitted-group-cool-20261029-ext4-batch8.jsonl).
