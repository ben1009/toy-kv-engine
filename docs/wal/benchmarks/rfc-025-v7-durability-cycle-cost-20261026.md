# RFC 025: full v7 durability-cycle feedback experiment, 2026-10-26

**Result: rejected and reverted. The broader timer produces no SSD batching-policy changes and fails the fixed SSD improvement gate.**

The subsequent [requested SSD rerun](rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026.md)
uses the exact same enabled candidate binary. Its separate and pooled results
preserve this original failure, every stall, and stronger historical peaks.

The previous [hash-stage and group-hint investigation](rfc-025-v7-large-group-hint-cache-20261026.md) left a measurement question: coalescing classifies only `fdatasync` latency, even though v7 must prepare and write an embedded FRONTIER before that call. This experiment measures that missing work and uses the broader duration as the next cycle's batching signal. It does not retain any of the previous rejected hash/offload or group-size policies.

## Candidate and timing scope

The timer starts immediately before FRONTIER preparation and stops after successful WAL sync and FRONTIER publication/credit cleanup, before updating the public durability watermark and notifying waiters. It includes FRONTIER encoding/digests, extent-readiness work or waits, the marker write, `fdatasync`, sync profiling, and post-sync marker cleanup. DATA preparation/writes, the coalescing wait itself, public-watermark publication, and waiter notification are outside the timer. The duration is feedback for a subsequent cycle, never evidence of durability.

V7 uses the full duration to choose between the existing 100- and 400-microsecond coalescing bounds at the unchanged 100-microsecond classifier threshold. V4 retains its `fdatasync`-only signal. First-cycle behavior, bounded result capture, admission, hashes, on-disk bytes, marker allocation, one-sync ordering, error poisoning, and recovery rules retain their existing implementation.

Two benchmark counters expose successful v7 Parallel cycle nanoseconds and `v7_durability_cycle_promotions`: a promotion means `fdatasync < 100 us` but the full cycle is at least `100 us`. It counts classifications that differ, rather than actual blocking waits; coalescing can return before its bound. The benchmark JSON omits these optional fields for other formats/modes. Existing `fdatasync_ns` and FRONTIER-write counters retain their original timing scopes.

The delayed-FRONTIER test injects a 20-ms marker-write delay, manually completes a real DATA prefix, and checks that the returned batching signal equals the recorded full duration and contains both nested I/O timers. It also checks that a no-work sync does not add timing, durable publication succeeds, transient credits are released, and profile reset clears the new counters. The ordinary live-runtime profile test and JSON test are extended.

## Fixed comparison

The [predeclared plan](artifacts/rfc-025-v7-durability-cycle-cost-20261026-plan.json) fixes all cells, arm order, guards, cooldowns, and the conditional confirmation before captures begin. The [driver](artifacts/rfc-025-v7-durability-cycle-cost-20261026-run.py), [analyzer](artifacts/rfc-025-v7-durability-cycle-cost-20261026-analyze.py), [binary identities](artifacts/rfc-025-v7-durability-cycle-cost-20261026-binaries.json), [environment](artifacts/rfc-025-v7-durability-cycle-cost-20261026-environment.json), and [complete summary](artifacts/rfc-025-v7-durability-cycle-cost-20261026-summary.json) preserve the protocol and measurements.

Each cell has four unscored warmups, then three tmpfs or five ext4 pairs with retained/candidate v4 and retained/candidate v7 arms. Odd pairs run old v4, new v4, old v7, new v7; even pairs reverse that order. Values are 1 KiB; batch64 cells use 750,000 puts, point cells 100,000 puts, and the SST target is 2 GiB. Sampling is every 10 batches or 100 point operations. Tmpfs uses `/dev/shm`; ext4 uses the local repository's `target` directory. Ext4 gets an initial 30-second cooldown and 10 seconds between runs; tmpfs uses 3-second gaps. Builds, tests, and profiles do not overlap scored runs. Fresh databases and their PITR repositories are removed after every run.

The candidate is built from `7a415451` with `cargo build --release --package kv-engine --bin write-perf --features bench`. Its SHA-256 is `698d294f9704b0a42210265f852025d49b27f640c70fcf6888857fca71670e32`. The retained executable is unchanged at `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`. The [zero-context rejected patch](artifacts/rfc-025-v7-durability-cycle-cost-20261026-candidate.patch) reconstructs all [128 captured source inputs](artifacts/rfc-025-v7-durability-cycle-cost-20261026-source-sha256.tsv); apply with `git apply --unidiff-zero`, or GNU `patch -p1`.

## Throughput, CPU, and latency

Rates below are per-arm medians in puts/s. Changes are medians of within-pair ratios; they can differ from ratios of the displayed arm medians. Normalized change is `(candidate v7 / candidate v4) / (retained v7 / retained v4)` within each pair. Warmups are excluded.

| Cell | Retained v7 | Candidate v7 | Paired raw change | V4-normalized change | CPU change | p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tmpfs batch64, 8 writers | 826,848 | 815,103 | -1.7% | +3.0% | +1.4% | +17.3% |
| tmpfs point, 8 writers | 140,146 | 140,621 | +2.6% | -0.7% | -3.1% | -7.8% |
| tmpfs point, 1 writer | 48,741 | 48,887 | +0.3% | +4.8% | +0.1% | -2.0% |
| ext4 batch64, 8 writers | 401,606 | 391,959 | -2.4% | +2.4% | +4.6% | -3.0% |
| ext4 point, 8 writers | 16,293 | 16,397 | +0.9% | -0.1% | -2.3% | +3.6% |

All fixed loss, CPU, and tail guards pass. The required material SSD gain fails: neither SSD cell achieves at least 2% paired raw throughput gain together with a nonnegative v4-normalized change. The conditional extra five-pair SSD confirmation is therefore not run. No extra captures are used to seek a passing result.

SSD variation remains substantial. The old/new v4 batch arm medians are 379,864 and 86,476 puts/s, while their peaks are 409,202 and 389,630. V4's batching policy did not change. This control instability limits interpretation of a small normalized gain; all stalls remain in the raw records and summaries.

## What the broader timer actually changes

The following means weight each scored candidate cycle equally by summing counters and dividing by the total sync count. They include slow runs. They are not medians of per-run means.

| Candidate cell | Sync cycles | Changed classifications | Mean full cycle, us | Mean fdatasync, us | Mean FRONTIER write, us |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs batch64, 8 writers | 8,871 | 1,510 (17.022%) | 38.26 | 0.19 | 1.71 |
| tmpfs point, 8 writers | 38,509 | 251 (0.652%) | 8.66 | 0.14 | 0.74 |
| tmpfs point, 1 writer | 300,000 | 18 (0.006%) | 3.57 | 0.10 | 0.49 |
| ext4 batch64, 8 writers | 11,200 | 0 (0.000%) | 572.09 | 374.55 | 149.36 |
| ext4 point, 8 writers | 63,694 | 0 (0.000%) | 285.68 | 246.17 | 16.46 |

The ten scored SSD v7 records cover **74,894 cycles with zero promotions**. For every measured candidate SSD cycle, the full-duration and `fdatasync`-only classifiers select the same short/long category. Zero promotions does not establish a minimum latency for every individual sync. The new policy therefore has no demonstrated batching effect on this SSD, while tmpfs batch classification changes in about 17% of cycles.

This corrects the earlier hypothesis: timing only `fdatasync` misses real FRONTIER work, but that gap does not explain these SSD results through the 100-us coalescing threshold. The measured extra I/O and preparation/cleanup cost still exists; changing the classifier does not remove it. These captures also do not establish that a prior rejected candidate's SSD variation was caused by coalescing.

Full-cycle totals include blocked time and overlapping work on other threads; the residual beyond the nested timers is not a pure CPU measurement. Counter totals cannot be added across independent pipeline stages to reconstruct wall time. The current experiment does not establish v4 parity.

## Scored peaks and historical references

| Matching longer ext4 cell | Current v4 control peak | Current retained v7 peak | Candidate v7 peak | Prior strongest v4 control | Prior strongest retained v7 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Batch64 puts/s | 409,202 | 428,581 | 418,049 | 412,503 | 480,373 |
| Point puts/s | 18,278 | 16,519 | 16,477 | 18,379 | 16,574 |

The [prior winning run identities](artifacts/rfc-025-v7-large-group-hint-cache-20261026-peaks.json) preserve those stronger references; all use the matching longer operation counts, 2-GiB SST target, and sampling cadence. The retained v7 winners use the same `614f0cb1` binary. Historical shorter cells remain separate at 20,420 point / 482,410 batch puts/s for v4 and 15,759 / 391,992 for retained v7; those use 50K/100K operations and a 1-GiB SST target. Peaks do not replace paired comparisons or prove parity.

## Captures and restoration

All **76 scored records and 20 warmups** remain, with 96 unique run IDs. No unscored profiles are mixed into this experiment. Each manifest includes arm, phase, pair, binary hash, raw hash, command, run ID, and observed NVMe temperature.

| Cell | Script | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs-batch64 | [script](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-batch64-20261026.jsonl) |
| tmpfs-point8 | [script](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-point8-20261026.jsonl) |
| tmpfs-point1 | [script](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-point1-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-point1-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-durability-cycle-cost-tmpfs-point1-20261026.jsonl) |
| ext4-batch64 | [script](artifacts/rfc-025-v7-durability-cycle-cost-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-durability-cycle-cost-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-durability-cycle-cost-ext4-batch64-20261026.jsonl) |
| ext4-point8 | [script](artifacts/rfc-025-v7-durability-cycle-cost-ext4-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-durability-cycle-cost-ext4-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-durability-cycle-cost-ext4-point8-20261026.jsonl) |

All three Rust files are restored byte-for-byte to `7a415451`, with fresh source modification times so Cargo rebuilds the restored code. The canonical `target/release/write-perf` is restored to retained SHA-256 `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`. The candidate executable remains separately saved for reproducibility. All benchmark databases, PITR scratch repositories, and source-reconstruction scratch files are removed.

The [verification script](artifacts/rfc-025-v7-durability-cycle-cost-20261026-verify.py) reconstructs the candidate, checks restored source/binaries and all arm orders, and regenerates the summary exactly. The [verification record](artifacts/rfc-025-v7-durability-cycle-cost-20261026-verification.json) records exact patch reconstruction, source/binary identity, and validation. The prototype full check passed 1,964 tests, zero skipped, under nextest `2e465065-dc41-4322-a633-d41239a6bb58`. After restoration, the fresh full check passed 1,963 tests, zero skipped and one slow compaction integration test, under nextest `166515a3-d572-4ce5-8f5e-6e4a3128ccad`; no retry was needed. Both checks include formatting, dependency sorting, Clippy, unused dependencies, and spelling. The explicit direct `typos` check is also run before committing the evidence. Logs stay under `target/wal-opt/20261026-v7-durability-cycle-cost`.

The eleven retained optimizations remain. V4 parity and the full RFC 025 Stage 8 qualification gate remain unmet.

[WAL documentation](../README.md).
