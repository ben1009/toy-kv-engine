# RFC 026 V8 admission-visible DATA groups (2026-11-02)

## Result and scope

The V8 DATA submitter now observes admitted membership before producers finish
encoding or framing. Keep this implementation for review as the RFC 026
group-visibility correction. Its end-to-end performance qualification remains
open. The latest binary has no eligible batch measurement in this screen;
both small-direct batch runs were flagged for storage stalls. Earlier prototype wins
must not be presented as measurements of the final binary.

The retained policy gives multi-frame heads with admitted successors up to
10 microseconds to join contiguous ready frames. A lone admitted ticket and
single-frame heads submit immediately. The producer that coordinates a group
refreshes its admitted cutoff during the existing wait, up to eight tickets,
then drains the work already ready and atomically releases packer ownership.
Other producers publish ready buffers without competing to pack that group.
The ticket high-water mark is published during admission, before encoding,
and is also the membership boundary for the immediate submission path.

The packer acquires coordinator state without holding its submission lock;
FRONTIER registration retains the opposite, established lock order. A failed
or panicking packer poisons the runtime and releases its ownership flag.
Unfinished successors cannot extend the optional deadline or bypass the head.
Buffers stay owned by pending submissions or the existing bounded I/O worker.
The implementation adds no worker thread and preserves the RFC digest and
DATA/FRONTIER durability protocol.

## Method and preserved variants

All runs use release `write-perf --features bench`, PITR V8 Parallel, ext4 on
`/dev/nvme0n1p3`, 1 KiB values, no compaction, a 2 GiB target SST and latency
sampling every ten operations. Batch-64 measurements contain 750,000 puts.
Point measurements contain 20,000 puts with eight or sixteen writers, and
5,000 puts with one writer. The baseline includes the preceding bounded
durability coalescing change and uses readiness-driven DATA submission.

The [plans and hashes](artifacts/rfc-026-v8-admitted-data-groups-20261102-plans.json)
identify each executable and fixed schedule. All 60 completed measurements,
including failed performance guards and storage outliers, are preserved in
the [raw records](artifacts/rfc-026-v8-admitted-data-groups-20261102-raw.jsonl).
The predeclared stall flag is p99 above 10 ms or mean fdatasync above 1 ms.
Passing this aggregate filter does not establish the absence of shorter or
isolated stalls. Some included samples have maxima above 30 ms.

The prototype sequence was:

- **100us:** admission-aware cooperative packing with a 100-microsecond join.
  Worker wakeups dropped substantially, but the clean sixteen-writer batch
  pair gained 10.5% throughput while p99 worsened 19.6%. Eight-writer point
  writes also lost throughput and p99. This policy is not retained.
- **10us:** shorten the join. It produced a 350,457 ops/s eight-writer batch
  sample with 2.322 ms p99, but solo batch throughput fell to 69,928 ops/s
  versus a 75,989 control. This is a distinct, unretained binary.
- **fast-path:** add the atomic admission-end marker and immediate submission
  for a lone admitted ticket. Its eligible batch confirmation pairs improved
  throughput and p99: eight writers, 329,631 / 3.308 ms to 337,753 / 2.837 ms;
  sixteen writers, 423,470 / 5.117 ms to 441,549 / 4.774 ms. Solo batch reached
  75,629 ops/s. However, two eight-writer point runs measured only 5,271 and
  6,217 ops/s versus controls near 9,000. They had 309 ms and 240 ms maxima
  and higher mean sync times, but pass the original aggregate filter. They
  remain included; this screen cannot separate storage effects from a
  submission-pattern effect. The universal join policy is not retained.
- **small-direct:** the retained binary additionally submits single-frame
  heads immediately, while selecting their window from admitted tickets.
  The preparation join applies to multi-frame heads. Results below refer only
  to this last executable and the preserved baseline.

These policy revisions and confirmations were adaptive follow-ups to observed
guard failures, not one independent, predeclared qualification cohort. Earlier
variant patches are archived separately alongside the
[retained patch](artifacts/rfc-026-v8-admitted-data-groups-20261102-candidate.patch).

## Retained-binary measurements

| Workload | Writers | Baseline best eligible ops/s | Baseline p99 ms | Retained ops/s | Retained p99 ms |
|---|---:|---:|---:|---:|---:|
| Batch 64 | 8 | 333,791 | 3.426 | excluded | excluded |
| Batch 64 | 16 | 423,470 | 5.117 | excluded | excluded |
| Point | 8 | 9,262 | 1.645 | 9,270 | 1.700 |
| Point | 16 | 9,327 | 4.093 | 8,792 | 3.674 |

Each retained workload has one measurement. Both batch measurements are
stall-flagged, so there is no retained-binary batch speedup claim. Baseline
batch eligibility is 3/7 at eight writers and 2/7 at sixteen. Baseline point
eligibility is 4/6 and 4/4, respectively. The retained point-eight throughput
is effectively equal to the fastest control, with 3.3% higher p99. The
retained point-sixteen score is 5.7% below the fastest control, with 10.3%
lower p99. These best scores come from different cohorts.

The eligible adjacent sixteen-writer point pair is 8,472 ops/s / 3.820 ms
versus 8,792 / 3.674: throughput +3.8%, p99 -3.8%. The adjacent eight-writer
point control was stall-flagged, so its pair is not scored. The retained
binary has no solo benchmark result; the earlier lone-ticket fast-path
measurement is prototype evidence only. The
[summary](artifacts/rfc-026-v8-admitted-data-groups-20261102-summary.json)
records eligibility, best samples and medians separately for every variant.

Prototype counters establish that admission-aware grouping changes the
submission shape: the fast-path batch confirmation reduced eventfd writes
from 7,135 to 3,967 at eight writers and from 6,392 to 4,051 at sixteen.
Those are not retained-binary performance measurements. The retained batch
measurements still need clean controls before comparing with V7 or V4, or
making an adoption decision.

## Correctness checks

All 146 V8-related tests pass outside the sandbox. New coverage verifies that
a group observes an admitted but unframed successor, grows when another
ticket is admitted while waiting, and submits one grouped worker command.
It also covers bounded progress with an unfinished successor, close/poison
and packer ownership release, and immediate submission for a single-frame
head despite a deliberately long test join budget. Existing out-of-order
producer, embedded FRONTIER-gap, crash, recovery and engine lifecycle cases
pass. All-feature/all-target Clippy, ordinary library checking and formatting
also pass.
