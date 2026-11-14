# RFC 026 V8 Coalescing Budget Sweep on GCP

**Date:** 2026-11-14 UTC

**Source commit:** `4cc8d596f33ce35ad9f916718c73931daa5d9bcc`

**Source patch SHA-256:** `d683ac59358c9473becd48af039159b4560d7babe85d219a3b71b794a954ebdd`

**Benchmark binary SHA-256:** `721b9ceaae5d273ca416aded683e13690dba9a02f131ca22a8ecb688f477899d`

## Finding

The 0 μs setting had the highest median throughput in the five-run score sweep, with a paired median increase of 4.16% over 400 μs and wins in four of five matched repetitions. It also used more CPU per put and about twice as many durability advances. The 400 μs setting had the lowest CPU per put and Sync count, and formed the largest groups.

The one-trace-per-budget diagnostics do not support a simple claim that larger groups create more WAL-return disorder: the aggregate intra-group inversion rate fell as budgets and group sizes increased, while the sampled MVCC out-of-order fraction rose. The trace is descriptive, and cannot establish that either ordering metric caused the throughput difference.

These data justify a repeated, lower-cost 0-versus-400 confirmation. They do not qualify a new default or support changing the WAL or MVCC protocol.

## GCP experiment

The runner completed 25 score runs and five separate diagnostic runs, for 30 successful runs total. Each score arm was repeated five times in counterbalanced order. All runs used 16 writers, 100,000 Point puts, and 1 KiB values. The V8 configuration used Direct FRONTIER and a 4 MiB Sliding Fallocate window. Only the benchmark-only coalescing-budget override changed between arms: 0, 50, 100, 200, or 400 μs.

Score runs did not enable commit-order tracing, MVCC sampling, or profiling. The benchmark retained its aggregate WAL counters and sampled operation latency every 100 completed operations. Each diagnostic run captured the first 1,024 complete commit records and sampled MVCC publication every 64 commits; those instrumented runs are not throughput scores.

The host was a 16-vCPU C4 instance with two Local SSDs in RAID0, ext4 mounted `rw,noatime,stripe=256`, and kernel `7.0.0-1017-gcp`. The Rust toolchain was `nightly-2026-09-23`.

## Score results

Medians across five unprofiled score runs per budget:

| Coalescing budget | Throughput (k puts/s) | CPU (μs/put) | Sync count | Puts/Sync | Put p50 (ms) | Sampled p99 (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 μs | 34.62 | 87.56 | 12,807 | 7.81 | 0.4412 | 1.0581 |
| 50 μs | 32.61 | 80.81 | 11,194 | 8.93 | 0.4857 | 1.3330 |
| 100 μs | 33.10 | 78.08 | 9,538 | 10.48 | 0.4635 | 1.3505 |
| 200 μs | 32.88 | 72.53 | 6,838 | 14.62 | 0.4769 | 0.9058 |
| 400 μs | 33.24 | 70.92 | 6,341 | 15.77 | 0.4734 | 1.0487 |

Paired throughput change against 400 μs, pairing by repetition:

| Budget | Median paired change | Wins against 400 μs |
| ---: | ---: | ---: |
| 0 μs | +4.16% | 4/5 |
| 50 μs | −1.57% | 0/5 |
| 100 μs | −0.30% | 2/5 |
| 200 μs | −1.09% | 2/5 |

The 0 μs result is a throughput-versus-work tradeoff: its median Sync count was 2.02× the 400 μs count, and its CPU per put was 23.5% higher. The 50/100/200 μs settings did not beat 400 μs consistently. Sampled p99 varied substantially between runs and is not evidence of a stable tail-latency change.

The override applies only when the existing eligible coalescing branch is reached; it does not add an unconditional delay to every durability cycle. The 0 μs setting removes the additional budget in that branch.

## Commit-order and MVCC diagnostics

Each budget had one instrumented trace. The commit-order trace contains 1,024 complete records per arm. It may end partway through a durability group; size-stratified statistics use only fully observed contiguous groups. Intra-group inversions compare WAL-return order for tickets covered by the same durable-frontier publication. Inter-group inversions compare tickets from different publications. Inversion rates are descriptive pairwise rates, not independent samples.

| Budget | Complete groups / complete 16-ticket groups | Intra-group WAL-return inversions | Inter-group WAL-return inversions | MVCC out-of-order samples | Entry / decision distance p50 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 μs | 137 / 0 | 1,965 / 3,999 (49.14%) | 0 / 519,776 (0%) | 65.0% | 2 / 2 |
| 50 μs | 121 / 5 | 2,130 / 4,711 (45.21%) | 0 / 519,064 (0%) | 67.3% | 3 / 2 |
| 100 μs | 103 / 21 | 2,290 / 5,687 (40.27%) | 0 / 518,089 (0%) | 69.8% | 3 / 3 |
| 200 μs | 86 / 38 | 2,416 / 6,730 (35.90%) | 0 / 517,046 (0%) | 75.2% | 5 / 4 |
| 400 μs | 65 / 62 | 2,073 / 7,626 (27.18%) | 0 / 516,150 (0%) | 75.4% | 3 / 3 |

At the same complete 16-ticket group size, the measured WAL-return inversion rates were 39.07% for 50 μs (234/599 pairs), 39.84% for 100 μs (1,004/2,520), 34.14% for 200 μs (1,557/4,560), and 26.63% for 400 μs (1,981/7,440). The 0 μs trace had no complete 16-ticket group. These rates come from one trace per budget and should not be treated as stable estimates.

Across the sweep, larger budgets produced more full 16-ticket groups and fewer Syncs, but the aggregate intra-group inversion rate declined rather than increased. Meanwhile, sampled MVCC out-of-order publication rose from 65.0% at 0 μs to 75.4% at 400 μs. This rules out a simple monotonic relationship between the measured inversion rates and throughput in this sample; it does not establish what mechanism explains either trend. The MVCC path fraction is a separate metric from commit-pair inversion rate.

All five diagnostic runs captured 1,024 records and 1,562 MVCC samples with zero dropped samples. Their instrumentation can affect scheduling, so their latency and throughput must not be mixed with the score results.

## Interpretation and next measurement

The sweep does not show that coalescing-induced WAL-return order is the cause of the Point throughput gap. A 0 μs budget is a promising throughput candidate for confirmation, while 400 μs remains more CPU- and Sync-efficient. The five repetitions are too few to justify a default change, especially given the latency variation.

The next experiment should repeat a counterbalanced 0-versus-400 score comparison with more repetitions and retain the same Direct FRONTIER and Sliding Fallocate settings. Repeat the trace diagnostics separately if the ordering question remains important; the current single trace per arm is insufficient for a causal conclusion. Do not change the MVCC sequencer or WAL notification protocol based on this sweep alone.

## Result verification and retention

The runner verified all 30 planned runs: `summary.json` reported `complete: true`, and the collection metadata recorded 30 planned and completed run IDs with successful `result.json` and matching `run-metadata.json` files. `remote-exit.json` reported `failed: false` and `runs_started: 30`.

The transferred result archive and checksum metadata were retained locally and matched at SHA-256 `a87db0921cbb534747da35629786e03a52fda30caeb57debaed6effa21d80d64`. The raw archive was not added to GitHub. The VM and temporary SSH firewall were left in place; no cloud cleanup or Terraform destroy was run. The experimental source patch is also local-only and is identified above by its checksum.
