# RFC 025: kernel worker pool limits, 2026-10-28

**Rejected.** Limiting both kernel helper classes to four works mechanically but does not improve the fixed SSD matrix. Point8 observes **-54.7% paired throughput / 7.99x p99** over three pairs below the aggregate stall thresholds. The accepted large-DATA async-dispatch source `f7f39b1b` and executable are restored. All four planned workloads complete.

## Diagnosis and corrected implementation

Two fixed, excluded diagnostics use the retained executable at eight and sixteen writers. A 100-ms task sampler observes up to **70 / 69 kernel helpers** belonging to the WAL issuer. At the sampled peaks they are sleeping in the kernel worker loop. This is a pool-size observation, not a measure of active device concurrency or proof that helper creation causes storage stalls. Native WAL calls are traced into memory and emitted after timing; no SSD SMART/temperature polling occurs.

The first prototype caps only bounded workers. Its real-ring setter/query test passes, but a separate engine probe still observes **231 helpers**. On the pinned Linux 6.12.71 path, forced-async requests can enter the work queue before file assignment; WRITE permits the unbounded classification when that file is not known. Limiting only the regular-file pool therefore does not cover this path. See the [dispatch implementation](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/io_uring.c), [WRITE definition](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/opdef.c) and [registration implementation](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/register.c). The [liburing registration manual](https://github.com/axboe/liburing/blob/master/man/io_uring_register.2) documents the two independently configurable worker classes. Exact downloaded source hashes remain in the interrupted-attempt record.

The corrected prototype requests **four bounded and four unbounded helpers per NUMA node**, only for the ext-filesystem v7 runtime already enabling large-DATA async dispatch. The dedicated ring issues WAL DATA writes. Other runtime setup stays the same; unavailable or denied tuning falls back to kernel defaults. This host has one NUMA node. A fixed excluded engine probe observes **four total helpers**, confirming the correction reaches the actual workload. Its heavy I/O stalls make its throughput unsuitable for scoring.

This cap also applies to ordinary small DATA requests that fall back to kernel helpers. It changes scheduling capacity even though their explicit async flag is unchanged. Encoded bytes, DATA reservations, FRONTIER order, coalescing, whole-WAL sync and ACK rules retain their implementation.

## Preserved interrupted attempt

A command-chain error launches the first prototype's matrix after its engine probe fails. Two warmups finish; the first scored baseline invocation is interrupted when the mistake is detected. Its process tree is stopped, metadata is saved and its disposable databases are removed. **No scored result from that attempt is used**, and its warmups are not pooled with the corrected matrix. The failed probe and both warmups remain. One interrupted process has no final benchmark JSON/run ID and is recorded separately.

The corrected launcher checks the probe's successful return before launching any scored work. Both source variants have separate patches, 128-input manifests, binaries, plans and logs. The initial check also flags a literal kernel thread name in the diagnostic script's spelling step while returning zero; the original log and script hash remain, and an equivalent prefix classifier passes a direct spelling check before capture. The corrected full check has no spelling error.

## Validation and fixed comparison

Both prototypes pass **1,974 tests** and **48 focused WAL tests**, zero skipped/retried. A real-ring test queries both worker limits, verifies ordinary setup is unchanged, and verifies the selected limits. Real O_DIRECT large/small writes, v7 extent/recovery, poison, short-write and ownership coverage also run. Formatting, Clippy, dependency checks and release builds pass outside the sandbox. The restored source passes **1,973 tests**, zero skipped/retried.

The base is `976a1e35`. Retained executable SHA-256 is `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8`; the corrected candidate is `420742d4ef2c5f68eada2244627c5a9b5c0335df482dd02c9e54d4e3ea1bfc8d`. The bounded-only prototype is `568b4d41efed45ed06c83af198448e826a020ca0214a0649d2492ed749583255`.

All commands use fresh local ext4 databases, explicit v7 Parallel, no compaction, 1-KiB values and a 2-GiB SST target. Batch64 writes 750,000 puts and samples every ten batches; point8 writes 100,000 puts and samples every hundred. Cooldowns are thirty seconds initially per cell and ten seconds between invocations. The sixteen-writer stage has two warmups and five alternating pairs; each other stage has two warmups and three pairs. All four run regardless of interim performance. Host audits before/during 1-Hz/after observe no recognized competing work. No build/test/profile/source edit overlaps capture; sampling cannot exclude every brief or unrecognized task.

The frozen mask flags mean FRONTIER writes >=1 ms or mean fdatasync >=1.5 ms, excluding a pair if either arm is flagged. This implements the request to ignore obvious stalls while preserving every raw observation. It misses short outliers and DATA/preparation delays, and cannot distinguish every source-induced I/O delay from environmental variation. Unflagged means below the thresholds, not stall-free.

| Workload | Matched pairs | Paired throughput | Paired p99 | Paired sync count |
|---|---:|---:|---:|---:|
| Batch64, 16 writers | 2 of 5 | -2.0% | +1.5% | +2.1% |
| Batch64, 8 writers | 3 of 3 | -4.4% | +3.9% | -0.5% |
| Point, 8 writers | 3 of 3 | -54.7% | +699.3% | +2.2% |
| Batch64, 1 writer | 3 of 3 | +4.6% | +2.2% | unchanged |

The sixteen-writer stage lacks the required three matched pairs and does not improve either metric. Batch8 and batch1 stay within their regression guards, but do not improve both throughput and p99. Point8 fails throughput and latency guards; CPU growth is diagnostic and does not determine rejection. DATA bytes match throughout. Raw sixteen-writer medians are +3.9% throughput / -1.7% p99, illustrating the influence of the excluded I/O-heavy pairs.

The point comparisons are:

| Pair | Baseline / candidate puts/s | Baseline / candidate p99 ms |
|---|---:|---:|
| 1 | 5,248 / 5,214 | 11.790 / 9.887 |
| 2 | 16,034 / 7,264 | 1.224 / 9.786 |
| 3 | 15,837 / 3,265 | 1.346 / 19.735 |

Mean syncs vary from 0.251 to 0.575 ms in pair3, still below the frozen flag. These observations do not isolate every storage or scheduling contribution or prove a universal 55% slowdown. They fail to support keeping this scheduling cap. Fewer sleeping helpers alone does not establish faster writes.

## Final state and evidence

The first variant contributes five completed invocations: two retained diagnostics, one failed-mechanism probe and two excluded warmups. The corrected variant adds one mechanism probe plus **36 matrix invocations**: 28 scored and eight warmups. This is **42 new completed run IDs**; adding the prior verified 549 yields **591** in this lineage. The single interrupted invocation without JSON is additional and remains explicitly recorded. Both variants' evidence stays separate. No v4-parity or RFC-adoption claim is added.

The modified worker file is restored byte for byte with a refreshed modification time before the restored full check. Canonical `target/release/write-perf` is atomically replaced with the retained async-dispatch executable, preserving hardlinked artifacts. This slice commits evidence only and stops for review.

- [Corrected plan](artifacts/rfc-025-v7-async-pool-limits-20261028-plan.json), [matrix](artifacts/rfc-025-v7-async-pool-limits-20261028-matrix.json), [environment](artifacts/rfc-025-v7-async-pool-limits-20261028-environment.json)
- [Corrected patch](artifacts/rfc-025-v7-async-pool-limits-20261028-candidate.patch), [128-input manifest](artifacts/rfc-025-v7-async-pool-limits-20261028-source-sha256.tsv), [binaries](artifacts/rfc-025-v7-async-pool-limits-20261028-binaries.json), [checks](artifacts/rfc-025-v7-async-pool-limits-20261028-checks.json)
- [Corrected engine probe](artifacts/rfc-025-v7-async-pool-limits-20261028-probe-captures.json), [probe output](artifacts/rfc-025-v7-async-pool-limits-20261028-probe-runs.jsonl)
- [Batch16](artifacts/rfc-025-v7-async-pool-limits-20261028-ext4-batch16.jsonl), [batch8](artifacts/rfc-025-v7-async-pool-limits-20261028-ext4-batch8.jsonl), [point8](artifacts/rfc-025-v7-async-pool-limits-20261028-ext4-point8.jsonl), [batch1](artifacts/rfc-025-v7-async-pool-limits-20261028-ext4-batch1.jsonl); each has matching commands/hashes and host-audit files
- [Analyzer](artifacts/rfc-025-v7-async-pool-limits-20261028-analyze.py), [summary](artifacts/rfc-025-v7-async-pool-limits-20261028-summary.json), [peaks](artifacts/rfc-025-v7-async-pool-limits-20261028-peaks.json), [decision](artifacts/rfc-025-v7-async-pool-limits-20261028-decision.json)
- [Initial diagnostics](artifacts/rfc-025-v7-async-worker-limit-20261028-diagnostic-captures.json), [initial patch](artifacts/rfc-025-v7-async-worker-limit-20261028-candidate.patch), [failed engine probe](artifacts/rfc-025-v7-async-worker-limit-20261028-probe-captures.json), [interrupted attempt](artifacts/rfc-025-v7-async-worker-limit-20261028-interrupted.json), [spelling correction](artifacts/rfc-025-v7-async-worker-limit-20261028-attempts.json)
- [Combined verifier](artifacts/rfc-025-v7-async-pool-limits-20261028-verify.py), [verification](artifacts/rfc-025-v7-async-pool-limits-20261028-verification.json)

[WAL documentation](../README.md).
