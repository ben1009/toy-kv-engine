# RFC 025: user-requested ext4 batch64 rerun, 2026-10-26

**Rerun: +0.8% raw throughput; -4.4% v4-normalized.**

The maintenance-state prototype remains reverted. Across all fifteen batch
pairs, raw throughput changes -3.8% and the normalized ratio changes
-4.4%. V4 parity remains unmet.

The user requested this fixed rerun after the maintenance-state prototype was
rejected. The [original experiment](rfc-025-v7-maintenance-snapshot-20261025.md)
retains its ten SSD batch pairs and rejection decision. This new capture is
reported separately, with all fifteen pairs pooled below for context. No Rust
source or executable was rebuilt or replaced for this rerun.

## Fixed comparison

Same host and ext4 SSD as the original experiment. Eight writers issue 100,000
puts in batches of 64, using 1-KiB values and a 1-GiB SST target. Both formats
use Parallel I/O; v7 explicitly enables PITR. Each run has a fresh database.
Setup and close are outside the measured write interval.

Four warmups precede five alternating four-arm pairs: retained/prototype
executables, each with v4 and v7. The initial cooldown is 30 seconds, with ten
seconds between runs. All 20 scored records and four warmups are kept. There
are no extra repeats selected by the results. No builds, tests, or CPU profiles
overlap these measurements. Batch latency has only 16 samples per run.

| Executable | SHA-256 |
| --- | --- |
| Retained immutable logical batches | `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9` |
| Rejected maintenance-state prototype | `91a5aa113886f2f91e575360c212ab0ac9c27a41835be4c49fea038a03c3a7aa` |

These are the exact executables from the original experiment. Its source
manifest and rejected patch remain the source provenance for the prototype.
The production source and default `target/release/write-perf` remain the
retained version.

## Rerun measurements

| Build / format | Median puts/s | Peak puts/s | Median CPU, ms | Sampled median p99, ms |
| --- | ---: | ---: | ---: | ---: |
| Retained build, v4 | 341,473 | 361,642 | 912.5 | 2.588 |
| Prototype build, v4 control | 356,476 | 357,602 | 901.7 | 2.577 |
| Retained build, v7 | 330,837 | 342,690 | 673.7 | 4.408 |
| Prototype build, v7 | 335,758 | 356,002 | 609.0 | 4.522 |

Ratios below are medians of paired prototype/retained ratios. Normalization
divides each v7 rate by its same-executable v4 control before comparing builds.
CPU is process user plus system time; lower CPU and latency ratios are better.

| Pair | Raw throughput | v4-normalized throughput | CPU | Sampled p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1.175x | 1.340x | 0.793x | 0.741x |
| 2 | 1.067x | 0.932x | 0.896x | 1.159x |
| 3 | 0.942x | 0.956x | 1.022x | 1.008x |
| 4 | 0.898x | 0.822x | 0.963x | 1.045x |
| 5 | 1.008x | 0.971x | 0.994x | 1.164x |
| Median | 1.008x | 0.956x | 0.963x | 1.045x |

Across all fifteen pairs, CPU changes -0.6%; sampled p50/p99
ratios are 1.042x/1.017x. Of fifteen pairs,
8 lose raw throughput and 10 lose normalized throughput.
The previous ten-pair capture was -5.8% raw / -3.8% normalized. The user-requested
rerun is additional evidence, and the original summary remains unchanged.

In this rerun the paired retained-v7/v4 throughput ratio is 0.975x;
the prototype-v7/v4 ratio is 0.947x. SSD temperature spans
31–32 °C across warmup and scored records. These controls and the short,
sparse latency samples limit the conclusions.

## Highest comparable scored batch64 results

| Build / format | Best puts/s |
| --- | ---: |
| Prior v4 Parallel | 482,410 |
| Prior retained v7 Parallel | 391,992 |
| Retained v7 in this rerun | 342,690 |
| Rejected prototype v7 in this rerun | 356,002 |

Historical and current peaks come from separate runs. The paired results keep
the moving v4 controls and stalls visible; a faster peak alone does not qualify
the RFC performance gate.

## Evidence

- [Driver](artifacts/rfc-025-v7-maintenance-snapshot-rerun-ext4-batch64-8writers-20261026-run.sh)
- [Commands, run IDs, hashes, and SSD temperatures](artifacts/rfc-025-v7-maintenance-snapshot-rerun-ext4-batch64-8writers-20261026-runs.tsv)
- [All raw records](artifacts/rfc-025-v7-maintenance-snapshot-rerun-ext4-batch64-8writers-20261026.jsonl)
- [Analyzer](artifacts/rfc-025-v7-maintenance-batch-rerun-20261026-analyze.py)
- [Summary](artifacts/rfc-025-v7-maintenance-batch-rerun-20261026-summary.json)

The analyzer checks exact executable hashes, raw hashes, complete pairs,
settings, sample and operation counts, and distinct IDs across all three batch
captures. It reproduces the separate rerun and pooled fifteen-pair summary.
The driver refuses to overwrite prior captures and removes its disposable
database directories. The execution log and protocol remain under
`target/wal-opt/20261026-v7-maintenance-batch-rerun`.

[WAL documentation](../README.md).
