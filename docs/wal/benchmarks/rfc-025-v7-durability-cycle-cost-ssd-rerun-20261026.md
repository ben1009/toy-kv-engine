# RFC 025: requested SSD rerun with full-cycle timing enabled, 2026-10-26

**Result: the saved timing candidate was rerun unchanged. The rerun does not establish a reliable SSD benefit, and the retained source/build remains in place.**

The maintainer requested an independent SSD rerun after the [initial experiment](rfc-025-v7-durability-cycle-cost-20261026.md) failed its improvement gate. This capture is separate from the original conditional confirmation, which was not triggered. The initial result and rejection remain recorded.

The executable actually used for the candidate arms is `target/release/write-perf-v7-durability-cycle-cost`, SHA-256 `698d294f9704b0a42210265f852025d49b27f640c70fcf6888857fca71670e32`. It contains the eleven earlier retained optimizations plus the full FRONTIER preparation/write, sync, and cleanup timing policy. The retained control is `target/release/write-perf-v7-validated-logical-batch`, SHA-256 `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`. Running the separately named candidate avoids using the restored canonical executable accidentally. Both binaries match the initial experiment byte-for-byte.

## Fixed rerun

The [plan](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026-plan.json), [driver](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026-run.py), [binary identities](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026-binaries.json), and [environment](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026-environment.json) were saved before captures. Each ext4 workload has four unscored warmups and exactly five paired rounds containing retained v4, candidate-build v4, retained v7, and candidate v7. Odd pairs run old v4, new v4, old v7, new v7; even pairs reverse that order.

Both workloads use eight writers, 1-KiB values, and a 2-GiB SST target. Batch64 uses 750,000 puts, sampled every ten batches; point writes use 100,000 puts, sampled every 100 operations. Ext4 is on `/dev/nvme0n1p3`; scratch databases stay under the local repository's `target` directory. The original 30-second initial cooldown and 10-second inter-run gaps are retained. No builds, tests, profiles, or other benchmarks overlap these captures. Every fresh database and its PITR repository is removed after its run.

There are **40 new scored records and eight warmups**. All original SSD captures are also preserved: the pooled comparisons contain ten pairs per workload, 80 scored records and 16 warmups, with 96 unique run IDs. No stalls are filtered and no additional attempts are selected based on results.

## Throughput, CPU, and latency

The rates in the next table are per-arm medians. Changes are medians of within-pair ratios and can differ from ratios of those arm medians. V4-normalized change is `(candidate v7 / candidate v4) / (retained v7 / retained v4)` within a pair. The pooled row includes all original and rerun pairs.

| Workload / captures | Retained v7 median puts/s | Candidate v7 median puts/s | Paired raw change | V4-normalized change | CPU change | p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Batch64, original five pairs | 401,606 | 391,959 | -2.4% | +2.4% | +4.6% | -3.0% |
| Batch64, requested five pairs | 405,872 | 379,833 | +1.6% | -6.4% | +12.7% | -2.2% |
| Batch64, all ten pairs | 403,739 | 388,713 | -0.4% | -2.0% | +7.1% | -2.6% |
| Point, original five pairs | 16,293 | 16,397 | +0.9% | -0.1% | -2.3% | +3.6% |
| Point, requested five pairs | 16,357 | 16,505 | +1.1% | -26.0% | -1.2% | +14.2% |
| Point, all ten pairs | 16,311 | 16,435 | +1.0% | -10.4% | -1.7% | +7.3% |

Fresh batch runs have a +1.6% paired raw median, but the candidate's per-arm median is lower and normalized throughput loses 6.4%. The pooled batch raw result is nearly flat (-0.4%), with 7.1% more CPU. Fresh point throughput rises 1.1% raw, while sampled p99 rises 14.2%. Pooling yields +1.0% raw point throughput and +7.3% p99. These results do not establish an SSD gain from the timing policy.

The moving v4 controls are unstable. In the requested point cell, old/new v4 medians are 12,654 / 17,545 puts/s, while their peaks are 17,816 / 17,774. Those variants retain the same v4 batching policy. The 38.7% median paired control ratio strongly affects normalization; the -26.0% normalized point result cannot be interpreted as a clean causal regression from the v7 change. All of these control stalls remain in the summaries and raw files.

## Scored peaks

These are the highest scored rates in the requested rerun only.

| Workload | V4 control peak, puts/s | Retained v7 peak, puts/s | Candidate v7 peak, puts/s |
| --- | ---: | ---: | ---: |
| Batch64 | 419,458 | 416,401 | 441,889 |
| Point | 17,816 | 16,473 | 16,612 |

The strongest matching references across the earlier investigation, the original experiment, and this rerun remain separate from the rerun-only peaks:

| Matching longer ext4 cell | Strongest v4 control | Strongest retained v7 | Strongest timing candidate v7 |
| --- | ---: | ---: | ---: |
| Batch64 puts/s | 419,458 | 480,373 | 441,889 |
| Point puts/s | 18,379 | 16,574 | 16,612 |

The [summary](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026-summary.json) includes each winning run ID, binary hash, raw-record hash, and manifest. The newer v4 batch control peak is preserved, and the stronger retained v7 batch reference of 480,373 puts/s is not replaced by the lower current measurements. All references above have the same longer operation counts, 2-GiB SST target, sampling cadence, and eight writers. The candidate peaks do not establish parity across workloads.

Historical short-cell references remain separate: 20,420 point / 482,410 batch puts/s for v4 and 15,759 / 391,992 for retained v7, using 50K/100K puts and a 1-GiB SST target.

## Did the enabled policy change SSD batching?

The promotion counter identifies cycles where the full duration chooses the 400-us coalescing bound while fdatasync alone would choose 100 us. Zero promotions means the classifications agree; it does not establish a minimum latency for every individual syscall.

| Capture / workload | Candidate sync cycles | Changed classifications | Mean full cycle, us | Mean fdatasync, us | Mean FRONTIER write, us |
| --- | ---: | ---: | ---: | ---: | ---: |
| requested rerun, Batch64 | 11,652 | 0 | 2353.75 | 1132.40 | 778.29 |
| original + rerun, Batch64 | 22,852 | 0 | 1480.54 | 760.97 | 470.04 |
| requested rerun, Point | 63,726 | 0 | 290.42 | 248.02 | 17.76 |
| original + rerun, Point | 127,420 | 0 | 288.05 | 247.10 | 17.11 |

All **75,378 new candidate SSD cycles** report zero promotions, bringing the combined result to **150,272 cycles with zero changed classifications**. The candidate code is enabled, but its broader feedback chooses the same batching categories as fdatasync alone in these captures. The full timer exposes extra work; it does not remove that work or explain the SSD gap through this threshold.

The cycle means weight all successful measured cycles equally and include slow runs. They are not medians of per-run means, and other pipeline stages can overlap these intervals. Extra time beyond the nested timers includes blocked time, preparation, profiling, and cleanup; it is not a pure CPU measurement.

## Captures and verification

| Requested cell | Script | Manifest | Raw records |
| --- | --- | --- | --- |
| ext4-batch64 | [script](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-ext4-batch64-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-ext4-batch64-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-ext4-batch64-20261026.jsonl) |
| ext4-point8 | [script](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-ext4-point8-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-ext4-point8-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-ext4-point8-20261026.jsonl) |

The [analyzer](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026-analyze.py) verifies raw-record SHA-256 values, binary identities, every expected arm and its order, counts, formats, options, and global run-ID uniqueness before producing separate and pooled comparisons. Input-file hashes are included in the summary, which regenerates exactly. The [verification record](artifacts/rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026-verification.json) checks saved and canonical binary hashes, unchanged Rust/build inputs, and scratch cleanup. The initial candidate's source patch and 128-input manifest remain linked from the original experiment; their reconstruction was verified before this rerun.

No Rust changes or rebuilds are made for this rerun. The canonical executable and retained source remain unchanged; the candidate stays separately saved. Validation checks artifact parsing, summary reproduction, report links, and direct `typos`. The earlier restored-source full check remains 1,963 passing tests; it is not rerun or counted as a new test run here. The eleven retained optimizations remain in place. V4 parity and the full RFC 025 Stage 8 gate remain unmet.

[Initial full-cycle experiment](rfc-025-v7-durability-cycle-cost-20261026.md) · [WAL documentation](../README.md).
