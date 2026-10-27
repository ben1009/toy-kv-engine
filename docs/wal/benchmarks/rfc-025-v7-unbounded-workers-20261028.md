# RFC 025: unbounded kernel worker limit, 2026-10-28

**Not retained.** Limiting only the unbounded kernel helper class to 16 passes the sixteen-writer screen, but fails the eight-writer and solo batch guards. The kept large-DATA async-dispatch source `f7f39b1b` and executable are restored; no new Rust optimization remains.

## Candidate and mechanism

The prior [worker-pool experiment](rfc-025-v7-async-pool-limits-20261028.md) observed up to 70/69 mostly sleeping helpers at eight/sixteen writers. Its four-per-class cap hurt point writes and did not qualify. This candidate changes only the unbounded class used by the pinned kernel for forced-async writes queued before file assignment. The bounded class retains its existing limit. Sixteen allows room for the sixteen synchronous benchmark writers; it is a test choice, not a general sizing rule. Helper counts alone do not establish active I/O concurrency or a performance bottleneck.

The registration runs only for ext-filesystem v7 WAL rings already enabling async dispatch for large DATA. Unavailable/denied registration logs at debug level and keeps kernel defaults. The real-ring test sets distinct initial limits `[3, 11]`, verifies ordinary setup leaves both intact, then verifies candidate setup yields `[3, 16]`. Existing tests issue large forced-async DATA beside an ordinary small direct write, validate exact bytes and lengths, and close successfully.

One fixed excluded engine probe observes **16 total helpers**, with at most five sampled in R/D states, against the predeclared prerequisite of at most 32 on this host. The earlier baseline diagnostic observed 69 at sixteen writers. These are different excluded diagnostics, not a paired performance result. The cap reaches the workload; the probe is not scored. Native WAL events are collected into memory, with 100-ms task samples and a sampled host audit.

The frozen plan inherits the registration manual’s per-NUMA terminology. More precisely, the pinned Linux 6.12.71 implementation maintains worker-class limits in the issuer’s `io_wq`; this experiment has one NUMA node and does not qualify behavior or scaling on multiple nodes. See the pinned [queue classification](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/io_uring.c), [registration](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/register.c), and [worker accounting](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/io-wq.c) implementations. Downloaded source hashes are preserved in the earlier interrupted-attempt record.

## Frozen comparison

The [plan](artifacts/rfc-025-v7-unbounded-workers-20261028-plan.json) precedes source edits and captures. Both arms use v7 Parallel on fresh repository-local ext4 databases: 1-KiB values, no compaction, 2-GiB SST target. Batch64 cells perform 750,000 puts, sampling every ten batch commits; point8 performs 100,000 puts, sampling every hundred calls. The device is the Solidigm SSDPFKNU010TZ on `/dev/nvme0n1p3`, kernel 6.12.71-1-lts.

Each cell starts with one warmup per arm, followed by five alternating pairs for batch16 and three pairs each for batch8, point8 and batch1. Initial cooldown is 30 seconds, with ten seconds between invocations. All four cells run regardless of intermediate performance. No replacements or selective repeats occur. The excluded mechanism probe must succeed before the matrix starts; a checked launcher enforces this dependency. No builds, tests, source edits or profiling overlap captures, and no SSD health/temperature polling occurs. The sampled host audits find no competing recognized workload.

The filter remains **mean FRONTIER write >= 1 ms or mean fdatasync >= 1.5 ms**. A pair is excluded if either arm is flagged; every raw record remains available. The screen requires at least three matched pairs with both paired-median and arm-median throughput above baseline and paired p99 below baseline. Other cells require at least two matched pairs, both throughput ratios at least 0.95, and paired p99 at most 1.10. DATA bytes must match; CPU and RSS are diagnostic.

These aggregate cutoffs exclude obvious long-I/O runs but miss shorter tails. Below-threshold samples are not established to be stall-free, and a source change can affect I/O timing. Ratios do not isolate storage/environment effects from the candidate.

## Results

Percentages are **median paired candidate/baseline ratios** after the declared filter. Arm-median throughput is calculated separately; it is not the same statistic.

| Workload | Matched pairs | Throughput | p99 | Sync count | Arm-median throughput | Check |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Batch64, 16 writers | 2, 3, 4, 5 | +1.2% | -6.3% | -1.0% | +5.1% | Pass |
| Batch64, 8 writers | 1, 2, 3 | -10.1% | -9.3% | +9.3% | -14.1% | Fail |
| Point, 8 writers | 1, 2, 3 | +49.8% | -88.3% | -0.8% | +50.0% | Pass |
| Batch64, 1 writer | 1, 2, 3 | -20.6% | +666.8% | +0.0% | -20.3% | Fail |

Batch16 excludes pair 1 because the baseline mean FRONTIER write is 1.673 ms. Its remaining four pairs pass the numerical screen; their baseline p99 values still span 14.3–21.3 ms, so this modest observed gain is not a clean isolation of helper overhead. No other scored run reaches the aggregate filter thresholds.

Batch8 improves p99 but misses both throughput guards: paired median -10.1%, arm-median -14.1%. Its arm medians are **306,389 → 263,337 puts/s**, **9.094 → 8.306 ms p99**, and **1,631 → 1,862 syncs**. Paired median sync count rises 9.3%, consistent with less effective amortization during this capture. The experiment does not prove why grouping changes. Reducing helper count is insufficient evidence for keeping the cap.

Point8 passes numerically, but baseline rates range from 3,244 to 15,633 puts/s and baseline p99 from 1.161 to 18.859 ms, all below the aggregate cutoffs. The first pair is 15,633 → 15,733 puts/s and 1.161 → 1.404 ms p99; it does not show the aggregate 49.8% throughput gain. The larger ratios in later pairs cannot be assigned solely to the cap.

Solo batches also fail: paired throughput -20.6% and p99 7.67x. Storage timing moves in both directions below the mask: pair 1 is 62,460 → 19,897 puts/s, while pair 3 is 18,255 → 66,190 puts/s. These observations are preserved and do not establish a universal source-only regression. They also do not supply the required evidence to retain the candidate.

### Every scored pair

Each metric cell shows **baseline → candidate**. Means and p99 are milliseconds. “Excluded” applies to the whole pair under the frozen filter. All warmups and the probe remain in raw artifacts.

| Workload | Pair | Puts/s | p99 ms | Mean FRONTIER ms | Mean sync ms | Excluded |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Batch64, 16 writers | 1 | 100,257 → 424,434 | 29.530 → 31.960 | 1.673 → 0.135 | 1.067 → 0.412 | Yes |
| Batch64, 16 writers | 2 | 396,812 → 503,501 | 20.118 → 6.030 | 0.137 → 0.132 | 0.526 → 0.398 | No |
| Batch64, 16 writers | 3 | 409,642 → 413,028 | 21.327 → 20.521 | 0.152 → 0.225 | 0.504 → 0.444 | No |
| Batch64, 16 writers | 4 | 446,329 → 434,680 | 15.073 → 13.755 | 0.103 → 0.131 | 0.503 → 0.451 | No |
| Batch64, 16 writers | 5 | 385,030 → 390,755 | 14.312 → 19.543 | 0.216 → 0.159 | 0.517 → 0.518 | No |
| Batch64, 8 writers | 1 | 339,699 → 403,454 | 8.938 → 2.758 | 0.148 → 0.033 | 0.433 → 0.307 | No |
| Batch64, 8 writers | 2 | 306,389 → 263,337 | 9.094 → 8.883 | 0.155 → 0.136 | 0.482 → 0.488 | No |
| Batch64, 8 writers | 3 | 291,614 → 262,267 | 9.157 → 8.306 | 0.143 → 0.133 | 0.504 → 0.492 | No |
| Point, 8 writers | 1 | 15,633 → 15,733 | 1.161 → 1.404 | 0.024 → 0.019 | 0.254 → 0.251 | No |
| Point, 8 writers | 2 | 3,244 → 15,804 | 18.859 → 1.214 | 0.332 → 0.019 | 0.601 → 0.251 | No |
| Point, 8 writers | 3 | 10,486 → 15,711 | 10.311 → 1.207 | 0.059 → 0.019 | 0.303 → 0.251 | No |
| Batch64, 1 writer | 1 | 62,460 → 19,897 | 1.409 → 16.431 | 0.027 → 0.581 | 0.318 → 1.225 | No |
| Batch64, 1 writer | 2 | 62,648 → 49,770 | 1.362 → 10.446 | 0.028 → 0.100 | 0.314 → 0.417 | No |
| Batch64, 1 writer | 3 | 18,255 → 66,190 | 17.097 → 1.400 | 0.653 → 0.028 | 1.312 → 0.312 | No |

## Validation and disposition

The candidate passes **1,974 full tests and 48 focused tests**, formatting, strict Clippy, dependency checks, spelling, and the release build outside the sandbox. All selected focused tests run without an I/O-unavailable return. After rejection, all 128 baseline source-input hashes are verified and the restored source passes a fresh full check with **1,973 tests**, no retries or skipped tests. The canonical executable is replaced atomically with the saved baseline; no stale rejected binary remains at the canonical path.

The exact candidate patch, 128-input manifests, saved executable identities, raw JSONL, per-run commands, child usage, host audits, excluded probe traces and samples, summary, descriptive peaks, decision and reproducible verifier are preserved. Candidate source changes only `kv-engine/src/wal/parallel/worker.rs`. The source base is `073deec2c27b8376106deda1ee12d417a0ca18cb`.

| Executable | SHA-256 |
| --- | --- |
| Retained baseline | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Rejected candidate | `ea66e4409a25e1759587f7d5a6b8e2cf26707054b0398255e661fd7289de2d96` |

The kept forced-async optimization remains **provisional**, as documented in its [retention note](rfc-025-v7-large-data-async-dispatch-20261027.md). This rejected limit does not qualify v4 parity or default activation.

This experiment adds **37 completed unique invocations**: one excluded mechanism probe, eight warmups and 28 scored runs. The measurement lineage advances from 591 to **628**, plus the previously recorded interrupted invocation without JSON/run ID. No new capture is interrupted.

Reproduce analysis and final-state verification:

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-unbounded-workers-20261028-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-unbounded-workers-20261028-verify.py
```

The [decision](artifacts/rfc-025-v7-unbounded-workers-20261028-decision.json), [summary](artifacts/rfc-025-v7-unbounded-workers-20261028-summary.json), [checks](artifacts/rfc-025-v7-unbounded-workers-20261028-checks.json), and [verification receipt](artifacts/rfc-025-v7-unbounded-workers-20261028-verification.json) bind the experiment. Ignored local binaries and diagnostic traces remain under `target/`; the checks and verifier require those retained local files.
