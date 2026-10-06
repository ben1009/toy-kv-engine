# RFC 024: low-concurrency parallel comparison, 2026-10-03

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Eight- and sixteen-writer throughput repeats consistently below the archived
parallel implementation: **-16.8%** and **-15.4%**, respectively. Four-writer
rotation still swings between fast and slow intervals. No case passes the
frozen full stability screen, because latency controls or session-wide spreads
also fail. This rerun establishes no incremental performance gain or RFC
adoption pass. Parallel WAL remains opt-in.

## Scope and frozen protocol

Compare the archived `067d4b09` parallel executable against the latest retained
parallel executable at `2c4b64679add0c092325bda0831baf897494d593`.
The older revision already contains early tuning; it is not the first
unoptimized RFC implementation. The current executable includes lifecycle,
shutdown, and error-handling changes after the `925ed07d` candidate in the
[September 30 comparisons](rfc-024-parallel-wal-benchmark.md#low-concurrency-comparison-rerun-2026-09-30).
This is a fresh comparison with current code, rather than an exact repeat of
that historical candidate. Both executables are archived release builds with
`bench`; their SHA-256 digests are checked before and after timing.

| Arm | Executable SHA-256 |
| --- | --- |
| Earlier parallel, A | `9d4ccd2f5917a1ab02e3e15d501aeab22b50c718fafe7330beb760e77f43bcb6` |
| Current parallel, B | `6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da` |

Use the existing NVMe-backed ext4 filesystem on Linux 6.18.9-arch1-2, PITR
off, 200,000 single puts per scored run, 1 KiB values, and every-tenth-operation
latency sampling. Each scored run has 20,000 latency samples. Four writers use
the earlier rotation workload's 1 MiB SST target; eight and sixteen use 1 GiB.

Each case has six ABBA/BAAB blocks, with two scored runs per executable per
block. Case order rotates between rounds. Before a case's first block, each
executable receives exactly one 50,000-put warmup. Every run uses a fresh
database and is followed by a fixed five-second idle gap. Executables and
raw outputs stay in RAM during timing. There are no concurrent builds, tests,
tracing, storage-setting changes, or controller administrative polling.
The quiet preflight process and pressure snapshots do not establish exclusive
device access.

All **72 scored runs and six warmups** finish in **1,549 seconds**. None are
excluded, retried, or extended after a failed control. Only this experiment's
fresh database trees and RAM scratch directory are removed afterward.

A block passes repeat controls only when both executables' within-arm
maximum/minimum throughput is at most 1.05 and p99 is at most 1.10. The full
stability screen additionally requires all six blocks to pass and each
executable's session-wide throughput spread to be at most 1.10 and p99 spread
at most 1.20. These are measurement checks, not replacements for the RFC gate.

## Results including every block

Changes are medians of all six block ratios. Each block divides the geometric
mean of its two B measurements by the geometric mean of its two A measurements.
Absolute throughput values are medians of all twelve scored runs per arm;
their quotient is not the paired estimate. CPU per put is process user plus
system CPU over the same 200,000 puts. The latency and CPU columns remain
descriptive where controls fail.

| Writers | Earlier puts/s | Current puts/s | Throughput change | p99 change | CPU/put change | Throughput controls | Latency controls | Both controls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4, rotation | 9,800 | 9,741 | -1.7% | +3.6% | +1.6% | 3/6 | 2/6 | 2/6 |
| 8 | 18,100 | 15,043 | -16.8% | +24.2% | +17.6% | 6/6 | 4/6 | 4/6 |
| 16 | 27,780 | 23,408 | -15.4% | +24.6% | +13.0% | 6/6 | 4/6 | 4/6 |

| Writers | All throughput block ratios, current/earlier | Exploratory throughput interval |
| --- | --- | --- |
| 4, rotation | 1.488, 0.662, 0.984, 0.443, 1.481, 0.981 | 0.552–1.484 |
| 8 | 0.830, 0.833, 0.826, 0.835, 0.847, 0.818 | 0.822–0.841 |
| 16 | 0.842, 0.822, 0.845, 0.856, 0.855, 0.848 | 0.832–0.855 |

Intervals use 20,000 percentile block-bootstrap resamples, seed 24. Six blocks
provide only an exploratory interval conditional on these observations.
The intervals cannot override failed controls or qualify adoption.

All eight- and sixteen-writer blocks pass throughput repeat checks, with
worst within-block spreads of 3.34% and 3.09%, respectively. Each executable's
session-wide throughput spread stays below 5.11%. Every block has lower
current throughput and higher CPU per put. The throughput regression against
these saved executables is repeatable in this session. Some latency controls
fail, and sixteen-writer session-wide p99 varies by up to 45.1%, so the full
stability result remains failed.

Rotation remains inconclusive. Earlier throughput ranges from 4,361 to
9,972 puts/s; current ranges from 4,265 to 9,875. Current p99 ranges from
1.269 to 17.994 ms. Same-executable throughput spreads reach 2.32 times
and p99 spreads reach 14.18 times. In round three, both current repeats stay
near 4.3k puts/s while both earlier repeats stay near 9.8k: that block passes
local repeats yet reports a 55.7% current throughput loss. Local repeat
agreement alone does not establish comparable storage states. Candidate
slow intervals remain included and cannot be dismissed as environmental
noise. The small median difference does not demonstrate parity or resolve
the [existing stall investigation](../environment/rfc-024-wal-stall-diagnosis.md).

## Synchronization counters

Both executables reach identical software peaks in every scored run:
4/8/16 in-flight groups and outstanding write SQEs at the respective writer
counts. These are software pipeline counters, not measured device queue
depth. Every run reports 200,000 completed WAL buffers and write CQEs.

| Writers | Earlier median sync count | Current median sync count |
| --- | ---: | ---: |
| 4, rotation | 50,889.5 | 52,079.5 |
| 8 | 25,323.0 | 40,515.5 |
| 16 | 13,902.5 | 21,551.0 |

Current performs about 60% more sync calls at eight writers and 55% more
at sixteen, using ratios of these median counts. Its median aggregate
`fdatasync_ns` increases from 6.84 to 9.70 seconds at eight and from
3.65 to 5.72 seconds at sixteen. More sync calls and fewer puts covered
per sync are a concrete batching suspect. This observation does not isolate
the responsible change or prove that it accounts for the entire regression.
No runtime optimization is made during this rerun.

## Verification and artifacts

The fixed protocol SHA-256 is
`3b4884964b308b118e8de4adedda4cd08d7856c1c29c5f27d46ff0b6a5d9096a`.
It and both executable hashes remain unchanged after timing. A separate
verifier reconstructs all eighteen block ratios directly from the raw
outputs, checks counts, arguments, controls, medians, exploratory intervals,
nonoverlap, fixed gaps, and scratch cleanup, and confirms the retained
Rust source and release executable are unchanged.

Local artifacts are in `target/rfc024-parallel-rerun-20261003-m6qjoh5f/`:
both executable snapshots, `source-manifest.json`, `preflight.json`,
`run.py`, `protocol.json`, all 78 records in `runs.jsonl`, raw stdout
and stderr, `blocks.jsonl`, `summary.json`, `completion.json`, `verify.py`,
`verification.json`, and `artifact-manifest.json`. The manifest records
artifact sizes and SHA-256 digests. Whole-device counters sampled at process
boundaries are contextual and do not attribute individual WAL request latency.

This documentation-only result is checked with formatting, spelling, and
whitespace validation. No Rust tests are rerun for the report.
