# RFC 025: v7 DATA packer refill experiments, 2026-10-26

**Result: both refill policies are rejected and reverted. The eleven retained optimizations remain; v4 parity and the complete Stage 8 gate remain unmet.**

This follows the [audited buffering experiments](rfc-025-v7-buffered-io-ssd-20261026.md). Three fresh, unscored user-CPU profiles of the retained direct-I/O executable motivate testing fewer DATA-group handoffs. SHA-256 in v7 batch packing accounts for 18.0% of user-CPU samples. Channel receives in v7 point packing, coordination and I/O account for 11.1%, 9.7% and 4.6%. These profiles include setup/close, and v4 also builds SSTs after the scored interval; sampled CPU does not isolate SSD elapsed time.

## Changes and decision

Broad refill frames the initially captured v7 group, takes one snapshot of admissions that arrived meanwhile, and frames those additional tickets into the same worker group. It keeps the eight-ticket cap, ticket order, poison boundary, FRONTIER gaps, resource ownership and endpoint-before-completion ordering. It adds no timer or wait. The logical SHA-256 remains in the ordered packer, and the DATA / FRONTIER / one-fdatasync / publication protocol is preserved.

Broad refill reduces median DATA groups from 75,361 to 52,675 for point writes and 8,132 to 4,670 for batch64. Batch throughput nevertheless loses 7.6% raw, with 8.6% more CPU. It fails the fixed SSD criteria; dependent RAM and confirmation cells are cancelled.

The separate small-write policy permits refill only when the initial group contains a DATA buffer below the existing 64-KiB extent-initialization threshold. Large-only groups use the original packing path. It also reuses the admission guard needed for endpoint publication, dropping/reacquiring it only when additional batches require framing. Point DATA groups fall from 73,263 to 52,286 and CPU falls 2.6%, but throughput is effectively flat (-0.04% raw). Batch64 changes -1.5% raw and -7.2% normalized. Its normalized batch floor and material-gain requirement fail, so its RAM and confirmation cells are also cancelled. The two policies are not pooled.

## Isolated SSD results

Each policy uses four unscored warmups and five alternating four-arm rounds per workload, with retained/candidate v4 controls and retained/candidate v7. Every run uses eight writers, 1-KiB values, a 2-GiB SST target, 100,000 point puts or 750,000 batch64 puts, and latency samples every 100 point operations or ten batches. There is a 30-second cooldown before each cell and ten seconds between runs. Databases live under the local repository target directory and are removed after each run.

Across both policies there are **80 scored records, 16 warmups, 96 unique run IDs and 921 clear host-process samples**. Recognized competing build/test/benchmark processes are checked before, once per second during, and after every command. No build, test or profile overlaps scoring. Sampling cannot rule out every shorter or unrelated task or storage stall. All slow runs remain included. Earlier buffering and historical measurements are not pooled into this comparison.

Rates are per-arm median puts/s; changes are medians of paired ratios. Normalization is `(candidate v7 / candidate v4) / (retained v7 / retained v4)` within each round.

| Policy / workload | Retained / candidate median | Raw change | V4-normalized | CPU change | p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Broad refill, Point | 7,288 / 16,092 | +2.1% | -1.6% | -4.2% | -4.1% |
| Broad refill, Batch64 | 399,833 / 364,257 | -7.6% | -6.1% | +8.6% | -1.8% |
| Small-write refill, Point | 15,697 / 15,710 | -0.0% | -0.3% | -2.6% | +0.1% |
| Small-write refill, Batch64 | 365,213 / 373,095 | -1.5% | -7.2% | -0.8% | -1.3% |

Broad point arm medians differ sharply because stalled runs occur in different rounds; they do not imply a 2.2x improvement. The paired median is +2.1% raw and -1.6% normalized. Small-write batch arm medians likewise answer a different question from the paired ratios. Every pair is preserved in the summaries.

The acceptance rules were saved before each candidate's measurements: both SSD workloads require paired raw, normalized and arm-median throughput ratios of at least 0.95 and paired p99 at most 1.10. At least one must gain 2% raw with normalized ratio at least one. Passing would trigger RAM guards and a fixed five-pair SSD confirmation. Neither reaches that stage.

## Highest matching scored rates

Warmups are excluded. These peaks use matching counts, writer count, payload, SST target and sampling cadence. They supplement all-run comparisons; a higher isolated peak does not override the failed criteria.

| Policy / workload | V4 control | Retained v7 | Candidate v7 |
| --- | ---: | ---: | ---: |
| Broad refill, Point | 18,189 | 16,628 | 16,831 |
| Broad refill, Batch64 | 398,240 | 415,056 | 433,171 |
| Small-write refill, Point | 17,180 | 15,726 | 16,041 |
| Small-write refill, Batch64 | 386,335 | 428,556 | 385,894 |

The [peak identities](artifacts/rfc-025-v7-packer-refill-20261026-peaks.json) retain every winning run ID, binary hash and raw-record hash. The earlier retained v7 batch high of 480,373 puts/s remains a historical reference, outside these qualification rounds.

## Source, captures and validation

Both source snapshots are relative to `225f671a`, with exact 128-input manifests and saved release executables built with feature `bench`. The retained executable SHA-256 is `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`. Broad refill is `b28a87cc259b16f183896ac21fcdffaeebd30cbaf4f63c6f0a11e0dd7cc738e3`; small-write refill is `30075729dbff118738d3514457cd40057d306b9b85377f50069921bcb09eb51c`.

| Policy | Plan | Source patch | Input hashes | Driver | Analyzer | SSD summary |
| --- | --- | --- | --- | --- | --- | --- |
| Broad | [JSON](artifacts/rfc-025-v7-packer-refill-20261026-plan.json) | [patch](artifacts/rfc-025-v7-packer-refill-20261026-candidate.patch) | [TSV](artifacts/rfc-025-v7-packer-refill-20261026-source-sha256.tsv) | [Python](artifacts/rfc-025-v7-packer-refill-20261026-run.py) | [Python](artifacts/rfc-025-v7-packer-refill-20261026-analyze.py) | [JSON](artifacts/rfc-025-v7-packer-refill-20261026-ssd-summary.json) |
| Small-write | [JSON](artifacts/rfc-025-v7-small-packer-refill-20261026-plan.json) | [patch](artifacts/rfc-025-v7-small-packer-refill-20261026-candidate.patch) | [TSV](artifacts/rfc-025-v7-small-packer-refill-20261026-source-sha256.tsv) | [Python](artifacts/rfc-025-v7-small-packer-refill-20261026-run.py) | [Python](artifacts/rfc-025-v7-small-packer-refill-20261026-analyze.py) | [JSON](artifacts/rfc-025-v7-small-packer-refill-20261026-ssd-summary.json) |

Every cell's raw `.jsonl`, `-runs.tsv`, `-host.jsonl` and `-run.sh` share `rfc-025-v7-{packer-refill,small-packer-refill}-initial-{ext4-point8,ext4-batch64}-20261026`. The summaries hash these captures and verify all options, row schedules, raw/audit hashes and clear samples.

The [profile driver](artifacts/rfc-025-v7-packer-refill-20261026-profile.py), [manifest](artifacts/rfc-025-v7-packer-refill-20261026-profiles.tsv), [raw records](artifacts/rfc-025-v7-packer-refill-20261026-profiles.jsonl) and [sample summary](artifacts/rfc-025-v7-packer-refill-20261026-profile-summary.json) preserve the three excluded diagnostics. Flat and caller reports share the same prefix and cell names.

Broad refill passes 1,965 full-check tests; small-write refill passes 1,966, each with zero skipped/retried tests and one slow compaction integration test. New tests cover FRONTIER gaps, the eight-ticket limit, closed-admission drain below poison, empty refill and large-only bypass. Existing I/O-failure and recovery coverage passes. Formatting, Clippy, dependency sorting, unused-dependency checks and direct spelling checks pass. The [check record](artifacts/rfc-025-v7-packer-refill-20261026-checks.json) retains exact validation IDs and local log hashes.

Both source changes and the canonical executable are restored to the retained baseline. A fresh restored-source full check passes 1,963 tests with zero skips/retries and one slow compaction integration test. The [verification script](artifacts/rfc-025-v7-packer-refill-20261026-verify.py) reconstructs both exact source snapshots, checks saved binaries and regenerates the SSD summaries. All benchmark and reconstruction scratch databases are removed.

[WAL documentation](../README.md).
