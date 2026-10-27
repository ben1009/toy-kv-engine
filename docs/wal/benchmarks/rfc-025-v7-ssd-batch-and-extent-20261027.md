# RFC 025: SSD extent lookahead and separate batch coalescing, 2026-10-27

**Result: neither bounded extent policy nor the batch-only coalescing policy is adopted. The original retained SSD runtime and executable are restored. Batch coalescing reduces sync count, but misses the frozen material-gain requirement; the kept async and serial improvements remain.**

This investigation returns to SSD writes after the allocator and BBTree trials. Its source base is `53f48459b15ae96d50df39ab0f5e7586579206fc`. System and SkipMap remain selected. Every candidate starts from the retained asynchronous v7 extent preparation (`0ffebd51`) and serial startup/hybrid policy (`71bd7c23`); candidates are not stacked on a failed predecessor.

## Fresh excluded diagnosis

Three retained-baseline SSD captures precede the source changes: v4 point8, v7 point8 and v7 batch64/8. They collect 452 / 492 / 482 user-CPU samples at 99 Hz with DWARF stacks and zero lost samples. Existing engine counters record preparation / FRONTIER write / fdatasync time of 379.5 / 642.2 / 4,015.5 ms for v7 point, and 1,058.8 / 2,391.4 / 2,969.7 ms for batch. These overlapping timers have different ownership and cannot be summed into wall time. User-CPU samples do not measure kernel service or off-CPU wait. The captures motivate hypotheses; they do not establish that extent preparation causes every SSD stall.

Batch needs separate treatment. Large DATA groups already skip zero initialization, while FRONTIER and mixed/small groups can still prepare extents. In the excluded eight-writer batch trace, 11,720 logical batches share 2,565 syncs: **4.57 batches/sync**, median captured ticket advance four and maximum eight. In 1,725 syncs (**67.25%**), the worker's completed prefix advances beyond the already captured target during fdatasync. The median extra completed ticket count across all syncs is one (two among just the syncs with later completion). These later tickets are outside the existing FRONTIER certificate and cannot be acknowledged by that sync. This suggests investigating coalescing before writing FRONTIER, not expanding ACK after sync.

The [batch diagnosis](artifacts/rfc-025-v7-ssd-sync-preparation-20261027-batch-diagnosis.json) is recomputed from the third raw profile, not inferred from scored runs. The three diagnostic records are reused across the policies and counted once. They never enter scoring.

## Fixed SSD comparison

All cells use ext4 `/dev/nvme0n1p3`, 1-KiB values, a 2-GiB SST target, fresh databases and explicit v4/v7 Parallel selectors. Each screen has one warmup per arm and five alternating four-arm rounds: retained v4, candidate v4, retained v7, candidate v7; even rounds reverse that order. There is a 30-second initial cooldown and ten seconds between commands. Builds, tests and profiles do not overlap scoring. Point8/16 use 100,000 puts and sample every 100; point1 uses 20,000 puts and samples every ten. Batch64 uses 750,000 puts and samples every ten batches. Validators recompute sample counts from per-writer remainders.

Each v7 cell requires paired and separate-arm median throughput at least 0.95x, paired CPU at most 1.05x, p99 at most 1.10x, sync count at most 1.05x and identical DATA byte counts. A material gain requires at least 2% paired throughput with no CPU increase. V4 controls and v7/v4 normalization are diagnostic and remain visible. Peak scores are descriptive and never replace the fixed comparisons. All completed observations, including stalls, remain; no score-selected retries occur. Policies have separate frozen plans and cohorts rather than pooled acceptance estimates. No RAM performance gate is substituted for SSD evidence.

Recognized competing build, test and benchmark processes are sampled before, once per second during, and after each command. This cannot rule out every short-lived or unrelated task. V4 post-measurement SST/drain work and v7 PITR close behavior can influence later device state despite the cooldown. Throughput and CPU cover the measured writes; Linux wait4 peak RSS covers the exact engine child's whole lifetime, including setup/close. Comparisons are within each format.

## Bounded extent policies

The retained concurrent small-write path prepares the required 128-KiB-aligned prefix, then requests background initialization through the remainder of the already allocated 1-MiB extent. The first prototype limits that request to `min(allocated_end, needed_prefix + 128 KiB)`; the second uses `+256 KiB`. Physical allocation, required foreground preparation, large-DATA skip, serial full-extent fallback, file caps, readiness publication, sticky errors and poison handling are unchanged. ACK, close and freeze do not depend on scheduling the background worker.

Smaller bursts might interfere less with DATA/FRONTIER I/O, but a continuing stream still initializes its consumed prefixes and may incur more foreground misses. The first prototype's fast matched point pair records 175.8 ms of preparation versus 58.4 ms for retained, with almost flat throughput; its highest point score is lower. The second policy tests fewer foreground misses as a distinct source hypothesis. It is not a retry replacing the first failed cohort.

### V7 results

Changes are medians of within-round candidate/retained ratios. Absolute throughput columns are separate arm medians; their quotient can differ sharply under moving stalls. CPU/p99/sync are lower-is-better. Point p99 covers one put; batch p99 covers one 64-put batch.

| Policy | Workload | Retained puts/s | Candidate puts/s | Paired throughput | CPU | p99 | Sync count | Guards |
|---|---|---:|---:|---:|---:|---:|---:|---|
| 128-KiB lookahead | Point8 | 10,973 | 16,177 | +47.5% | -4.2% | -83.2% | -0.7% | Pass |
| 128-KiB lookahead | Batch64 / 8 | 382,006 | 95,859 | +1.1% | +1.4% | +8.7% | +2.6% | Fail |
| 256-KiB lookahead | Point8 | 10,161 | 5,067 | -3.2% | +3.0% | +25.5% | +0.3% | Fail |
| 256-KiB lookahead | Batch64 / 8 | 115,303 | 383,860 | -1.4% | -0.8% | -4.6% | +1.7% | Pass |

The 128-KiB policy fails the batch separate-arm throughput floor (candidate arm median is 0.251x retained), despite paired throughput +1.1%. The 256-KiB policy fails point separate-arm throughput (0.499x) and paired p99 (+25.5%). Its batch cell passes regression guards but has no material paired throughput gain. Both fixed screens fail, so each cancels its six planned 1/16-writer and confirmation cells. Neither policy is retained.

### V4 controls

| Policy | Workload | Retained puts/s | Candidate puts/s | Paired throughput | CPU | p99 | Sync count | Guards |
|---|---|---:|---:|---:|---:|---:|---:|---|
| 128-KiB lookahead | Point8 | 5,270 | 5,071 | +1.5% | -1.2% | -0.2% | +0.1% | Pass |
| 128-KiB lookahead | Batch64 / 8 | 377,637 | 381,110 | -1.2% | +4.7% | -4.5% | +4.3% | Pass |
| 256-KiB lookahead | Point8 | 18,111 | 7,383 | -2.0% | +1.9% | +20.7% | +0.0% | Fail |
| 256-KiB lookahead | Batch64 / 8 | 388,310 | 78,866 | -2.7% | +3.0% | +21.3% | +1.5% | Fail |

Both 128-KiB v4 control cells pass. Both 256-KiB v4 controls fail separate-arm throughput and p99 guards, demonstrating substantial session variation even in a format whose extent policy did not change. These controls limit causal interpretation of individual ratios. The failed v7 gates still prevent retention; no failure is waived because another arm stalls.

### Highest scores within each cohort

| Policy | Workload | Retained v4 | Candidate v4 | Retained v7 | Candidate v7 |
|---|---|---:|---:|---:|---:|
| 128-KiB lookahead | Point8 | 18,075 | 18,341 | 16,729 | 16,297 |
| 128-KiB lookahead | Batch64 / 8 | 392,731 | 386,286 | 435,334 | 389,907 |
| 256-KiB lookahead | Point8 | 18,359 | 18,221 | 16,480 | 16,143 |
| 256-KiB lookahead | Batch64 / 8 | 396,865 | 380,395 | 439,072 | 397,526 |

Winner run IDs, raw hashes and executable identities are in each policy's peak record. V7's highest point scores fall 2.6% / 2.0%, and highest batch scores fall 10.4% / 9.5%, for 128 / 256 KiB respectively. No historical peak is substituted into paired estimates.

## Separate large-batch coalescing

The third candidate starts again from the original retained source. After a sync of at least 100 microseconds, it permits an 800-microsecond coalescing deadline only when the initial contiguous unsynced wave consists entirely of single-ticket completions of at least 64 KiB. A conservative Boolean folds as contiguous completions advance; out-of-order groups do not classify a prefix. Multi-ticket groups do not expose individual batch sizes and therefore disable the extension, as do small batches. A new wave resets at the durable boundary.

Point/mixed/unknown waves keep 400 microseconds after slow syncs; fast v7 syncs keep 100 microseconds; v4 and the first cycle keep their previous policy. The coalescer still exits immediately when the admission prefix is complete and preserves one fixed deadline. It adds no compulsory grace at one writer. A later completion during coalescing may join the captured prefix; completions after FRONTIER capture still need a later certificate. Encoded bytes, buffer ownership, sync ordering and ACK rules do not change.

This differs from the [historical v4 600-microsecond experiment](../experiments/rfc-024-native-sync-batching-20261006.md), which was rejected, and from the [v7 fast-sync lookup experiment](rfc-025-v7-large-group-coalescing-20261026.md), which skipped waiting after cheap syncs and failed its CPU screen. Here only known large v7 waves after slow syncs receive a longer maximum; eligibility uses coordinator state without an admission-map scan. Neither prior failure is treated as evidence of a new win.

The frozen batch-only plan first scores batch8 for five rounds. Passing guards and a material gain are required before three-round point8, batch1/16, point1/16 and batch8 confirmation. Both screen and confirmation must show the material batch gain before retention. Remaining cells are cancelled on failure.

| Format | Retained puts/s | Candidate puts/s | Paired throughput | CPU | p99 | Sync count | Regression guards |
|---|---:|---:|---:|---:|---:|---:|---|
| v4 | 102,761 | 140,614 | -13.3% | +5.7% | +5.8% | -1.2% | Fail |
| v7 | 82,000 | 81,536 | +3.3% | +0.5% | -23.5% | -17.1% | Pass |

The v7 regression screen passes, with 3.3% higher paired throughput, 23.5% lower p99 and 17.1% fewer syncs. Paired CPU is **1.0050x**, however, so it misses the predeclared material-gain requirement of throughput at least 1.02x with CPU at most 1.00x. The six dependent cells are cancelled; there is no point/solo/16-writer or independent confirmation evidence for this candidate. The 0.5% CPU increase is not characterized as a proven regression: it simply fails the fixed no-increase condition. The failed v4 control also prevents a strong causal performance claim. No threshold is loosened after seeing results.

Median sync count changes from 2,682 to 2,248; both have 11,720 logical batches and identical DATA byte counts. This is about 4.37 versus 5.21 batches per median sync count. Median FRONTIER bytes change from 10,985,472 to 9,207,808. These counters support a separate batch policy investigation, while the timing remains sensitive to device state.

The highest retained v7 run in this cohort is 98,403 puts/s (`1793070224605`), versus 472,665 for the candidate (`1793070399534`). All five retained runs in this cohort are substantially slower than earlier identical-source runs. The highest retained v7 batch8 observation across the three cohorts is 439,072 puts/s, compared with 472,665 for the batch candidate (+7.7%). That cross-cohort peak comparison is descriptive, includes different device states, and does not override the paired or CPU gate. The saved binary is the same retained `e7f77128…` across all three cohorts.


## Correctness and identities

Both extent prototypes pass **1,970 tests** and nine focused real-file tests outside the sandbox. The ready-prefix test verifies the bounded background target and an untouched sentinel just beyond it while the file remains allocated to 1 MiB. A new test covers aligned/partial 192-KiB caps, continued writes, close and exact recovery. Existing tests cover paused-worker demand/close, large DATA cancelling zeroing, sticky failure, serial rollover and extent expansion.

The batch prototype passes **1,971 tests** in `cargo make check`, including formatting, Clippy, dependency and spelling checks, with zero skips/retries and one unrelated slow leveled-compaction test. Twelve focused tests execute with immediate output outside the sandbox. A real encoded-file fixture checks out-of-order completion, small/mixed and unknown multi-ticket waves, reset after a durable cutoff, no early durability advance, resource release, close and exact recovery. The first fixture attempt used a 65,536-byte value, exceeding the existing 65,535-byte live limit; the corrected fixture uses 65,535 bytes and still produces a DATA batch above the 64-KiB threshold. That failed pre-score test log remains recorded. No scored run used the faulty fixture.

| Executable | SHA-256 |
|---|---|
| Retained async + serial SSD policy | `e7f7712825d2166c14b5b67601388a8e29697f8fc034832e457061cf6e30ae19` |
| 128-KiB lookahead | `800bdc851fc2a4767a4cb56fbe81eb5d06dc3544c0784eb0da8d6b782be7f81d` |
| 256-KiB lookahead | `1afef7472b7bdadbf590db3899e09d6f6cb920e9d35537785eb6b736d83440da` |
| Batch-specific 800-microsecond deadline | `f7fd96e5ca0ecaaaa23f59f135e7ce9430521f986834e5c95f2d32070e378d54` |

Each candidate uses the same release build command, `cargo build --offline --locked --release --package kv-engine --bin write-perf --features bench`. Exact patches and 128-input manifests reconstruct each candidate from the base; no external diff renderer output is used. The first two restoration records describe actual historical restorations between policies. The last verifier owns the final live-source/executable check; original failed candidates and their measurements remain available.

The final runtime source is byte-identical to `53f48459`, including both kept SSD fixes. The canonical `target/release/write-perf` is atomically restored to `e7f77128…`; all three candidate executables and exact patches remain separately saved. A fresh outside-sandbox `cargo make check` on restored source passes **1,969 tests**, with zero skips/retries. No new production source commit, default change, PR or push is made. V4 parity and the complete Stage 8 gate remain unqualified.

Across all three policies, the record contains **100 scored invocations, 20 warmups and three excluded diagnostic invocations**, all with distinct IDs. Each extent policy contributes 40 scored and eight warmup records; the batch policy contributes 20 and four. Audit sample totals and exact reconstruction checks are in the chained verification records. Both original failures are preserved rather than relabeled by the later trial.

## Evidence

Each linked artifact family also contains the matrix, environment, executable identities, source manifest, checks, raw JSONL, per-invocation TSV and process audit, regenerated summary, peaks, decision and restoration. Local saved binaries, perf data and validation logs under `target/wal-opt` are required for local hash checks and are not repository payloads. Benchmark databases and reconstruction scratch are removed.

| Policy | Frozen plan | Exact patch | Summary | Highest scores | Verifier |
|---|---|---|---|---|---|
| 128-KiB lookahead | [plan](artifacts/rfc-025-v7-ssd-sync-preparation-20261027-plan.json) | [patch](artifacts/rfc-025-v7-ssd-sync-preparation-20261027-candidate.patch) | [summary](artifacts/rfc-025-v7-ssd-sync-preparation-20261027-summary.json) | [peaks](artifacts/rfc-025-v7-ssd-sync-preparation-20261027-peaks.json) | [script](artifacts/rfc-025-v7-ssd-sync-preparation-20261027-verify.py) |
| 256-KiB lookahead | [plan](artifacts/rfc-025-v7-ssd-two-chunk-lookahead-20261027-plan.json) | [patch](artifacts/rfc-025-v7-ssd-two-chunk-lookahead-20261027-candidate.patch) | [summary](artifacts/rfc-025-v7-ssd-two-chunk-lookahead-20261027-summary.json) | [peaks](artifacts/rfc-025-v7-ssd-two-chunk-lookahead-20261027-peaks.json) | [script](artifacts/rfc-025-v7-ssd-two-chunk-lookahead-20261027-verify.py) |
| Batch coalescing | [plan](artifacts/rfc-025-v7-ssd-batch-window-20261027-plan.json) | [patch](artifacts/rfc-025-v7-ssd-batch-window-20261027-candidate.patch) | [summary](artifacts/rfc-025-v7-ssd-batch-window-20261027-summary.json) | [peaks](artifacts/rfc-025-v7-ssd-batch-window-20261027-peaks.json) | [script](artifacts/rfc-025-v7-ssd-batch-window-20261027-verify.py) |

[Excluded profile metadata](artifacts/rfc-025-v7-ssd-sync-preparation-20261027-profiles.json) · [Raw diagnostic records](artifacts/rfc-025-v7-ssd-sync-preparation-20261027-profiles.jsonl) · [WAL documentation](../README.md).
