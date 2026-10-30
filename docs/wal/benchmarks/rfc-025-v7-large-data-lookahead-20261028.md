# RFC 025: unused-suffix preparation after large DATA, 2026-10-28

**Not retained.** Preparing the unused allocated suffix after concurrent large DATA increases zero-fill traffic and fails the fixed SSD batch checks. The retained large-DATA async-dispatch implementation and canonical executable are restored exactly.

## Hypothesis and candidate

The retained large-DATA path allocates a bounded extent, skips zero initialization and advances its shared preparation cursor through the entire allocation. Unused space is then also considered usable, so later small FRONTIER writes can still target unwritten extents. The existing native traces show preparation and FRONTIER I/O cost, but do not prove that extent conversion explains every delay.

The candidate instead skips only through the concurrent large group's actual reserved end, then requests background initialization of the unused suffix in the same allocated extent. Large DATA still initializes its own bytes. The existing shared preparation lock prevents background writes from overwriting the protected prefix. Foreground demand can prepare its own required range without waiting for the background worker. Serial demand, small-write policy, the physical cap, sticky failures, stopping/join behavior, DATA/FRONTIER bytes and sync-before-ACK rules retain their policies.

This differs from prior lookahead-size and sync-priority experiments: it changes which unused bytes large DATA leaves eligible for preparation. It adds no allocation beyond the already charged extent. Preparing more future bytes could reduce later conversion work but necessarily adds write traffic; the SSD comparison decides whether that trade helps.

## Excluded mechanism diagnostics

Two fixed native captures use the retained and candidate executables, in that order, at eight batch64 writers. The RAM-buffered interposer timestamps synchronous writes and syncs; the observer samples thread state every 100 ms and competing work every second. Both captures write the same 816,021,504 DATA bytes. They are excluded from scoring.

| Primary-WAL trace | Retained | Candidate |
| --- | ---: | ---: |
| Background zero writes | 76 | 6,116 |
| Background zero bytes | 9,961,472 | 763,236,352 |
| Foreground zero bytes | 6,684,672 | 5,595,136 |
| Total zero-fill MiB | 15.875 | 733.215 |
| FRONTIER / sync cycles | 1,953 | 1,767 |

The candidate demonstrably prepares far more bytes in advance. This is synchronous zero-fill traffic observed on the primary WAL inode, separate from DATA writes; it is not total physical SSD write amplification. The baseline diagnostic has severe delays: mean FRONTIER write 3.150 ms and sync 3.926 ms, versus 0.074/0.320 ms for the candidate. Its apparent throughput advantage cannot be used as a measured source benefit. Sampled kernel helper counts peak at 19 versus 869, mostly sleeping; those counts do not measure device queue depth or establish a cause of the batch regressions.

The [diagnosis](artifacts/rfc-025-v7-large-data-lookahead-20261028-diagnosis.json) recomputes counts from SHA-bound native traces and also retains overlap observations. These whole-inode traces include setup/close where present; the selected primary inode's FRONTIER and sync counts equal the engine's timed counters in both captures.

## Frozen SSD protocol

The [plan](artifacts/rfc-025-v7-large-data-lookahead-20261028-plan.json) precedes source edits and captures. Both arms use explicit v7 Parallel, fresh repository-local ext4 databases on the Solidigm SSDPFKNU010TZ, 1-KiB values, no compaction and a 2-GiB SST target. Batch64 cells perform 750,000 puts and sample every ten batch calls; point8 performs 100,000 puts and samples every hundred calls. Host: Linux 6.12.71-1-lts, `/dev/nvme0n1p3`.

Batch8 has two warmups and five alternating pairs. Batch16, point8 and batch1 each have two warmups and three alternating pairs. All four cells run regardless of interim performance, with thirty-second initial cooldowns and ten-second gaps. No source edit, build, test, profile or SSD health polling overlaps the captures. Sampled host audits detect no recognized competing workload. There are no replacement runs or score-selected repeats.

The unchanged aggregate stall flag is **mean FRONTIER write >= 1 ms or mean fdatasync >= 1.5 ms**. Either arm flags its whole pair; every raw observation is preserved. Batch8 requires at least three matched pairs, paired and arm-median throughput above baseline, and paired p99 below baseline. Other cells require at least two pairs, both throughput ratios >= 0.95 and paired p99 <= 1.10. DATA bytes must match. CPU/RSS are diagnostic, without a veto.

## Results

Changes are medians of paired candidate/baseline ratios after the declared filter; arm-median throughput is separate.

| Workload | Matched pairs | Throughput | p99 | Sync count | Arm-median throughput | Check |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Batch64, 8 writers | 2, 3, 4, 5 | -7.7% | +32.9% | +6.4% | -13.7% | Fail |
| Batch64, 16 writers | 2 only | -13.9% | +19.6% | +16.9% | -13.9% | Insufficient pairs; bounds also fail |
| Point, 8 writers | 1, 2, 3 | -59.4% | +108.6% | +2.0% | -78.4% | Fail |
| Batch64, 1 writer | 1, 2, 3 | -4.5% | +4.3% | +0.0% | +39.6% | Regression bounds pass |

The batch8 screen excludes pair 1 because of the baseline stall; four matched pairs remain and fail the benefit requirements. Batch16 excludes pairs 1 and 3, both arms flagged in each. Its single remaining pair is **606,948 → 522,489 puts/s**, p99 **4.065 → 4.860 ms**. That pair is descriptive, insufficient for the declared minimum.

The aggregate mask misses shorter delays. In point pair 2, rates are **16,304 → 16,412 puts/s**, p99 **1.229 → 1.297 ms**; other point candidates suffer much longer delays below the mask. Solo pair 2 favors the candidate strongly while pair 3 favors baseline. Small point writes and solo batches retain their preparation policies, so the table does not establish a universal 59% source-only point slowdown or a 40% solo gain. Storage/scheduling variation remains unresolved; no new filter removes those inconvenient observations.

The experiment demonstrates extra preparation work without a reliable SSD benefit. It does not isolate extra zeroing as the sole cause of the throughput losses, qualify the retained forced-async dispatch, or establish v4 parity.

### Every scored pair

Values show baseline → candidate; times are milliseconds. Exclusion covers the whole pair. Warmups and diagnostics remain in separate raw records.

| Workload | Pair | Puts/s | p99 ms | Mean FRONTIER ms | Mean sync ms | Excluded |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Batch64, 8 | 1 | 25,410 → 325,244 | 97.662 → 3.677 | 5.186 → 0.071 | 6.447 → 0.341 | Yes |
| Batch64, 8 | 2 | 312,270 → 331,409 | 7.980 → 3.197 | 0.131 → 0.069 | 0.365 → 0.323 | No |
| Batch64, 8 | 3 | 322,872 → 212,868 | 8.834 → 12.711 | 0.138 → 0.208 | 0.428 → 0.505 | No |
| Batch64, 8 | 4 | 269,319 → 284,320 | 8.189 → 10.516 | 0.181 → 0.127 | 0.537 → 0.481 | No |
| Batch64, 8 | 5 | 334,467 → 264,074 | 8.242 → 11.322 | 0.187 → 0.202 | 0.468 → 0.462 | No |
| Batch64, 16 | 1 | 40,684 → 86,528 | 64.470 → 70.881 | 5.882 → 1.166 | 4.333 → 2.599 | Yes |
| Batch64, 16 | 2 | 606,948 → 522,489 | 4.065 → 4.860 | 0.099 → 0.075 | 0.347 → 0.389 | No |
| Batch64, 16 | 3 | 93,553 → 39,865 | 44.294 → 84.340 | 2.585 → 2.384 | 2.174 → 5.096 | Yes |
| Point, 8 | 1 | 8,657 → 3,519 | 9.620 → 20.070 | 0.088 → 0.261 | 0.336 → 0.590 | No |
| Point, 8 | 2 | 16,304 → 16,412 | 1.229 → 1.297 | 0.017 → 0.017 | 0.249 → 0.253 | No |
| Point, 8 | 3 | 16,361 → 2,922 | 1.187 → 19.448 | 0.021 → 0.327 | 0.250 → 0.679 | No |
| Batch64, 1 | 1 | 24,190 → 23,098 | 11.423 → 11.917 | 0.110 → 0.091 | 1.364 → 1.490 | No |
| Batch64, 1 | 2 | 19,076 → 61,585 | 16.241 → 2.305 | 0.606 → 0.028 | 1.336 → 0.413 | No |
| Batch64, 1 | 3 | 67,373 → 33,764 | 1.288 → 11.665 | 0.026 → 0.270 | 0.313 → 0.705 | No |

## Correctness, restoration and evidence

The candidate passes **1,974 full tests and 48 focused WAL tests**, with no retry or selected I/O-path skip, plus formatting, strict Clippy, dependency/spelling checks and the release build outside the sandbox. The full check reports one slow test. The updated real-ext4 fixture preserves acknowledged large DATA/FRONTIER bytes while a queued initializer clears a canary in the unused suffix. The new fixture starts with large DATA, makes progress with the suffix worker paused, and closes/replays both large and small batches exactly under full and partially aligned physical caps. Existing tests cover serial rollover, sticky errors, poison and sealing joins.

The restored source matches all **128 baseline build-input hashes**. The canonical executable is restored by atomic replacement, and the Rust source mtime is refreshed before a fresh full check. That check passes **1,973 tests** with zero skips, after one simple-leveled-compaction test times out at thirty seconds and passes its automatic retry. The post-check no-flaky assertion therefore stops despite the successful full command. The timeout log is preserved; one isolated invocation with retries disabled also passes. No engine change is made for that timeout.

| Executable | SHA-256 |
| --- | --- |
| Retained baseline | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Rejected candidate | `d7d4bf83da62c8a8344b0757f8269c83e058a4b6dee8c6f7e82cc98888466e08` |

The source base is `8ca9a3d60f43804d4bd69535e32de85916514c78`. Only `wal/parallel/runtime.rs` differs in the preserved [candidate patch](artifacts/rfc-025-v7-large-data-lookahead-20261028-candidate.patch). This is an evidence-only slice; the prior [large-DATA async-dispatch optimization](rfc-025-v7-large-data-async-dispatch-20261027.md) remains provisionally retained.

This adds **38 completed unique invocations**: two excluded diagnostics, eight warmups and 28 scored observations. The lineage advances from 665 to **703**, plus the previously preserved interrupted invocation without JSON/run ID. No new capture is interrupted. Disposable benchmark databases are removed; exact executables, native traces and check logs remain under ignored local `target/` paths.

Reproduce the analysis and verify source/capture identities:

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-large-data-lookahead-20261028-diagnose.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-large-data-lookahead-20261028-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-large-data-lookahead-20261028-verify.py
```

The [summary](artifacts/rfc-025-v7-large-data-lookahead-20261028-summary.json), [checks](artifacts/rfc-025-v7-large-data-lookahead-20261028-checks.json), [decision](artifacts/rfc-025-v7-large-data-lookahead-20261028-decision.json) and [verification receipt](artifacts/rfc-025-v7-large-data-lookahead-20261028-verification.json) retain the result. The artifact family also includes the fixed matrix, capture drivers, raw JSONL/TSV records, host audits, 128-input manifests, executable identities, native-trace metadata and descriptive scored peaks.
