# RFC 025: producer DATA framing and post-submit hashing, 2026-10-29

**Combined candidate rejected.** Fresh batch throughput and p99 checks pass, but the one- and sixteen-writer point guards regress both metrics. The eight-writer point throughput also narrowly misses its bound. All five changed Rust files and the canonical retained executable are restored. The exact candidate binary and source patch remain available; the result does not reject the pipeline design or establish that readiness bookkeeping caused the point regressions.

## Change and correctness

The candidate combines the separately measured post-submit SHA overlap stage with producer DATA framing. Requests whose aligned DATA storage is at least 64 KiB return an owned frame job after ticket and physical offset assignment. MVCC sequencing and the seal-index update finish before the producer builds frames and their CRCs. Small requests retain packer framing. The canonical logical bytes, SHA256 prefix definition, physical frame layout, and one-sync publication protocol are unchanged.

Producer completions may arrive out of order. Only contiguous ready tickets enter the existing packer queue; later jobs wait in a charged ready map. The packer submits ready DATA and hashes the logical inputs in ticket order. The coordinator requires both contiguous completed DATA and the matching prefix snapshot before constructing FRONTIER. A job that fails, panics, or is dropped poisons its ticket. Close stops admission, waits for assigned framing jobs or poison, then drains and joins the existing runtime. No preparation thread pool is added.

An admitted batch keeps logical input ahead of its pinned aligned buffer in destruction order. Its reservation covers both allocations, including an outstanding producer job that survives poisoned close and persistent-image cleanup. The production concurrency test pauses one producer after admission and verifies that another producer can frame and return its WAL-only ticket while the first remains blocked, with exact seal-index and recovered commit timestamps. Runtime tests cover out-of-order readiness, unfinished-job cleanup, injected error/panic, DATA completion before prefix readiness, and close/recovery behavior.

## Method

All three batch cells are mandatory. Each uses one warmup per arm followed by four alternating four-arm rounds: retained v4, retained v7, the exact rejected P0 hash-overlap executable, and the combined candidate. This distinguishes the P1 increment from standalone P0 and compares the composition against retained v7. Conditional point guards use the frozen three-arm schedule only if all batch screens pass. No results select additional repeats.

Ranking uses the highest-throughput eligible scored invocation per exact binary and workload, with its own p99 and process CPU. The P0 control is excluded from retained historical ranking by executable hash. Historical cohorts remain separate and unequal. Mean FRONTIER writes of at least 1 ms or mean fdatasync of at least 1.5 ms independently flag an invocation; aggregate means can miss stalls.

The candidate passes 1,979 full tests and 59 focused tests, without retries or full-suite skips, plus formatting, strict Clippy, dependency checks, direct spelling checks, and release build outside the sandbox. Earlier fixture and unused-wrapper corrections remain in the check receipt. Source, builds, tests, profiles, and other benchmarks stay frozen during capture. The frozen plan accidentally retains a two-file restoration phrase from P0; the combined candidate actually changes five Rust files, all bound by the source manifests and patch.

## Fresh best scores

Each row selects the highest-throughput eligible scored invocation per exact executable. Its p99 and process CPU come from that same invocation. Point throughput has a 2% guard allowance and p99 a 5% allowance; batch screens require both metrics to improve or remain equal. CPU is diagnostic, not a veto.

| Workload | Retained v7 puts/s | Combined puts/s | Throughput | Retained / combined p99 ms | CPU | Eligible retained / combined | Check |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| batch1 | 65,231 | 80,853 | +23.9% | 1.358 / 1.250 | -17.2% | 4 / 4 | Pass |
| batch8 | 338,196 | 373,959 | +10.6% | 3.345 / 2.200 | +18.8% | 2 / 4 | Pass |
| batch16 | 408,874 | 573,732 | +40.3% | 20.006 / 4.521 | +6.2% | 3 / 3 | Pass |
| point1 | 2,237 | 2,069 | -7.5% | 0.758 / 1.241 | -4.3% | 3 / 3 | Fail |
| point8 | 15,378 | 15,068 | -2.0% | 1.186 / 1.157 | +2.1% | 3 / 3 | Fail |
| point16 | 23,874 | 22,150 | -7.2% | 1.667 / 1.879 | +16.0% | 2 / 2 | Fail |

The point guards are a measured rejection under the frozen schedule, not an isolated causal explanation. Several low-rate runs have visible tail latency despite passing the aggregate stall mask. No extra repeats or replacement scores are selected to rescue the candidate.

## Producer-framing increment over the P0 control

This view uses the same fresh cohort and the exact P0 executable, separately from the retained baseline.

| Batch64 writers | P0-only puts/s / p99 ms | Combined puts/s / p99 ms | Throughput | p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 62,660 / 2.238 | 80,853 / 1.250 | +29.0% | -44.1% |
| 8 | 414,564 / 3.146 | 373,959 / 2.200 | -9.8% | -30.1% |
| 16 | 568,835 / 4.530 | 573,732 / 4.521 | +0.9% | -0.2% |

At eight writers, the combined winner submits 11,316 groups, of which 11,033 are solo, versus 7,012 / 3,725 for retained v7's fresh winner and 7,272 / 4,276 for P0's winner. DATA buffers and puts are unchanged. Removing framing from the packer changes queue accumulation and produces many more one-batch submissions. This adds coordination work and is a plausible offset to the CPU-stage benefit; the comparison does not isolate all CPU or p99 causes. No coalescing interval, worker policy, or allocator is changed in this trial.

## Strongest matching-binary reference

The retained column includes all indexed historical and fresh scores for its exact executable. The P0 control is excluded by executable hash. These cohorts are unequal and provide a stronger reference, not a controlled source-effect estimate.

| Batch64 writers | V4 Parallel best puts/s / p99 ms | Retained v7 best | Combined best |
| --- | ---: | ---: | ---: |
| 1 | 85,618 / 1.195 | 72,647 / 1.373 | 80,853 / 1.250 |
| 8 | 388,372 / 2.511 | 478,798 / 2.653 | 373,959 / 2.200 |
| 16 | 595,941 / 2.967 | 606,948 / 4.065 | 573,732 / 4.521 |

The combined solo high improves 11.3% over the retained historical high and remains 5.6% below v4 throughput. At eight and sixteen writers, combined throughput remains below the stronger retained historical highs. These results do not qualify v4 parity or default activation.

## Reproduction and disposition

The [plan](artifacts/rfc-025-v7-producer-framing-20261029-plan.json) and [matrix](artifacts/rfc-025-v7-producer-framing-20261029-matrix.json) complete **96 invocations: 75 scored and 21 warmups**. Batch cells write 750,000 puts in batch64; point guards write 20,000 puts. All use 1-KiB values, a 2-GiB SST target, sampling every ten calls, fresh databases under the local repository, a 60-second initial cooldown per cell, and 15-second gaps. The databases are removed after capture. One-second host audits find no recognized competing builds, tests, profiles, or benchmarks. Aggregate masks do not establish stall-free storage.

The [checks](artifacts/rfc-025-v7-producer-framing-20261029-checks.json) preserve the final 1,979 full tests and 59 focused tests, plus earlier fixture and unused-wrapper corrections. After rejection, the five Rust files are restored with fresh modification times; the canonical executable is replaced through a fresh inode. A fresh full check passes **1,973 tests**, followed by direct spelling verification. The [disposition](artifacts/rfc-025-v7-producer-framing-20261029-outcome.json) records the restoration.

The retained executable is `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8`; the combined executable is `8bcc3df029c84a57991b6eba54b209d7121246b97fe4732df5e2da123059ea8f`; the P0 control is `27079b2afcd608d455a9bb91ccdad04f1567337b54896f4a486f0c51ccdd54e0`. The [zero-context patch](artifacts/rfc-025-v7-producer-framing-20261029-candidate.patch) reconstructs the [128-input candidate manifest](artifacts/rfc-025-v7-producer-framing-20261029-source-sha256.tsv) from `74979714`. Wire bytes and the prefix digest definition remain unchanged.

The [driver](artifacts/rfc-025-v7-producer-framing-20261029-run.py), [analyzer](artifacts/rfc-025-v7-producer-framing-20261029-analyze.py), [fresh summary](artifacts/rfc-025-v7-producer-framing-20261029-summary.json), [historical comparison](artifacts/rfc-025-v7-producer-framing-20261029-best.json), and [verifier](artifacts/rfc-025-v7-producer-framing-20261029-verify.py) reproduce the comparison. Raw captures: [batch1](artifacts/rfc-025-v7-producer-framing-20261029-ext4-batch1.jsonl), [batch8](artifacts/rfc-025-v7-producer-framing-20261029-ext4-batch8.jsonl), [batch16](artifacts/rfc-025-v7-producer-framing-20261029-ext4-batch16.jsonl), [point1](artifacts/rfc-025-v7-producer-framing-20261029-ext4-point1.jsonl), [point8](artifacts/rfc-025-v7-producer-framing-20261029-ext4-point8.jsonl), [point16](artifacts/rfc-025-v7-producer-framing-20261029-ext4-point16.jsonl). The [receipt](artifacts/rfc-025-v7-producer-framing-20261029-verification.json) adds 96 unique completed invocations to the inherited 5,103, for **5,199**, and keeps the inherited interrupted invocation separate.
