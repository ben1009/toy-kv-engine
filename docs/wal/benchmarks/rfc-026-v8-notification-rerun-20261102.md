# RFC 026 V8 notification retention and rerun (2026-11-02)

## Status

The combined notification separation and published-cursor implementation is
restored and kept locally at the maintainer's explicit request. The source
matches the prototype that passed `./dev check` with 2,151 tests passed and
32 skipped. No runtime tuning, format-default change, push or merge is part
of this rerun.

The [original report](rfc-026-v8-notification-split-20261102.md), its 96 samples,
and its initial negative retention decision remain available. The current
rerun uses exactly the original baseline and combined-candidate executables,
verified against their archived SHA256 values. No compilation or tests run
during the scored measurements.

The fresh cohort reruns batch-64 and point writes at one, eight and sixteen
writers in two rounds with reversed workload order. It contains 24 runs.
Because concurrent arms were missing eligible samples, one bounded follow-up
adds 12 runs: batch-eight, point-eight and batch-sixteen, reversing both
workload and arm order in its second round. Each cohort starts with a fixed
45-second cooldown and waits three seconds between runs. The first cohort's
arm order for a given case stays fixed; its reversed workload order alone
does not remove that ordering limitation. No further cohorts or tuning were
performed in this rerun.

Batch cases contain 750,000 puts; point cases use 5,000 puts at one writer and
20,000 at eight or sixteen writers. Other settings remain Parallel I/O, 1 KiB
values, no compaction, a 2 GiB target SST, latency sampling every ten operations
and normal counters without publication CPU scopes or detailed WAL diagnostics.
The storage is ext4 on the Solidigm P41 Plus. Data stays in the local repository.

The unchanged stall flag is p99 above 10 ms or mean fdatasync above 1 ms.
Each comparison selects the highest eligible throughput **within that cohort**
and takes p99, CPU and context counters from that same sample. No table fills a
missing arm from an earlier cohort. All raw samples are retained. Missing
eligible arms remain unqualified; retention does not depend on hiding
unfavorable runs. Passing the aggregate flag does not exclude isolated stalls
or establish statistical significance.

## Results

The complete rerun contains **36 measurements, 18 stall-flagged**. The first
cohort has 14 flagged samples; the focused confirmation has four. All 36
commands complete successfully. The
[measurement archive](artifacts/rfc-026-v8-notification-rerun-20261102-measurements.jsonl)
contains the full benchmark JSON, context counters, command/status records,
sample names and original record hashes. The
[summary](artifacts/rfc-026-v8-notification-rerun-20261102-summary.json)
also records eligible counts and medians.

### First cohort

Cells use the best eligible throughput per arm, with that run's p99 and total
process CPU. An absent score means no eligible sample, rather than zero.

| Workload | Writers | Baseline puts/s | Kept puts/s | Baseline p99 ms | Kept p99 ms | Baseline CPU s | Kept CPU s | Eligible baseline / kept |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Batch 64 | 1 | 69,453 | 69,088 | 1.336 | 1.341 | 6.378 | 6.353 | 1/2 / 1/2 |
| Batch 64 | 8 | 328,506 | — | 3.243 | — | 8.166 | — | 1/2 / 0/2 |
| Batch 64 | 16 | 423,321 | — | 4.502 | — | 10.708 | — | 1/2 / 0/2 |
| Point | 1 | 1,577 | 1,591 | 1.310 | 1.235 | 0.856 | 0.825 | 2/2 / 2/2 |
| Point | 8 | — | — | — | — | — | — | 0/2 / 0/2 |
| Point | 16 | 8,785 | 14,212 | 3.841 | 3.196 | 7.131 | 3.940 | 1/2 / 1/2 |

The prior solo batch regression does not repeat at the same magnitude:
throughput is down 0.5%, p99 up 0.4% and CPU down 0.4%. With one eligible
sample per arm, this is a small observed difference, not proof of equivalence.
Solo point throughput rises 0.9%, p99 falls 5.8% and CPU falls 3.7%.

Point-sixteen improves 61.8% in throughput, 16.8% in p99 and 44.7% in CPU.
Its sync count also falls from 2,458 to 1,407, and actual eventfd writes fall
from 9,026 to 7,465. Only one sample per arm is eligible, so these larger
changes need more evidence before being treated as a stable effect size.

### Focused confirmation

This separate cohort fills the missing batch-sixteen and point-eight arms.
It does not qualify batch-eight: all four batch-eight runs are flagged.

| Workload | Writers | Baseline puts/s | Kept puts/s | Baseline p99 ms | Kept p99 ms | Baseline CPU s | Kept CPU s | Eligible baseline / kept |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Batch 64 | 8 | — | — | — | — | — | — | 0/2 / 0/2 |
| Batch 64 | 16 | 428,909 | 488,630 | 4.777 | 4.091 | 10.177 | 8.951 | 2/2 / 2/2 |
| Point | 8 | 9,489 | 9,783 | 1.750 | 1.952 | 3.919 | 2.959 | 2/2 / 2/2 |

Batch-sixteen improves **13.9% throughput, 14.4% p99 and 12.0% CPU** in the
best-score comparison. Voluntary context switches fall **38.6%**, from 158,862
to 97,582; syncs fall 8.1%, from 1,485 to 1,364. Actual eventfd writes rise
4.2%, from 4,071 to 4,243. The optimization therefore does not establish a
general reduction in worker eventfd writes.

The direction repeats in both adjacent batch-sixteen comparisons:

| Order | Baseline puts/s | Kept puts/s | Baseline p99 ms | Kept p99 ms | Baseline CPU s | Kept CPU s |
|---|---:|---:|---:|---:|---:|---:|
| Baseline then kept | 425,363 | 483,044 | 4.643 | 4.119 | 10.303 | 9.025 |
| Kept then baseline | 428,909 | 488,630 | 4.777 | 4.091 | 10.177 | 8.951 |

Point-eight has a tradeoff in its best-score comparison: throughput rises
3.1% and CPU falls 24.5%, but p99 rises **11.5%**. The other adjacent pair
improves p99, from 1.607 to 1.483 ms, so the tail result is mixed. The table
does not substitute that lower-p99 run for the highest-throughput run.
Context-switch counters were not measured for point writes.

## Retention and limits

**Keep the combined implementation locally.** The new batch-sixteen
confirmation repeats the throughput, p99 and CPU improvements; solo batch
results are much closer than in the original rejection. The notification
separation and published-cursor path address the measured coordination cost
without changing digest, wire format, DATA grouping or durability policy.

This remains maintainer retention with incomplete performance qualification.
Batch-eight has no eligible kept sample in either new cohort, point-eight's
best-throughput sample has a worse p99, and point-sixteen has one eligible
sample per arm. These reruns do not establish V4/V7 parity or justify enabling
V8 by default. The original measurements and their initial rejection remain
unchanged. No performance result automatically reverts the requested kept code.

## Reproduction and checks

The executables are unchanged from the original experiment:

| Arm | SHA256 |
|---|---|
| Baseline | `5387771a5278302e65236fbfac65856c87b7e51bb37be5f0e9e62c05ac19279a` |
| Kept combined candidate | `937a3843a044e107e15db756674be151875e388ff3a0a392cbd4e97463bbd2aa` |

The original experiment archives the combined runtime patch and temporary
context probe. No benchmark-only probe is restored into the working source.
The [restoration record](artifacts/rfc-026-v8-notification-rerun-20261102-restoration.json)
binds the three restored source files to the exact checked prototype. Their
hashes and source snapshots still match after the rerun. That prototype
passed `./dev check`: **2,151 passed, 32 skipped**, plus formatting, dependency
sorting, Clippy, unused-dependency and typo checks. The full suite is not
repeated for an identical source restoration. Formatting is checked again
after the rerun. The [current check record](artifacts/rfc-026-v8-notification-rerun-20261102-checks.json)
also records passing typo checks, raw-result and best-score validation,
source/executable hashes, artifact copies, Python syntax and relative links.

The [first plan](artifacts/rfc-026-v8-notification-rerun-20261102-plan.json)
and [runner](artifacts/rfc-026-v8-notification-rerun-20261102-run.py),
[confirmation plan](artifacts/rfc-026-v8-notification-rerun-20261102-confirmation-plan.json)
and [runner](artifacts/rfc-026-v8-notification-rerun-20261102-confirmation.py),
and [archive script](artifacts/rfc-026-v8-notification-rerun-20261102-archive.py)
are preserved. Local paths refer to
`target/rfc-026-v8-notification-rerun-20261102/` and the preceding experiment's
executables; adjust those paths when reproducing elsewhere.
