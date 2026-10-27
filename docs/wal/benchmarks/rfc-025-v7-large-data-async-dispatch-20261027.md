# RFC 025: async dispatch for large v7 DATA, 2026-10-27 UTC

This candidate is **not retained: the SSD screen is inconclusive**. Only two matched pairs survive the predeclared stall mask, below the required three. Those two observe **+13.2% throughput / -61.4% p99**, but baseline p99 remains 16–18 ms despite mean I/O calls staying below the aggregate thresholds. The gain is not isolated from storage variation. Dependent guards are cancelled; the accepted source and executable are restored.

## Candidate and validation

The [v7 timing investigation](rfc-025-v7-16-writer-tail-diagnosis-20261027.md) locates substantial delay before DATA staging. An [older v4 native diagnostic](../experiments/rfc-024-native-async-kernel-dispatch-20261006.md) shows forced async can shorten submission calls while adding helper handoff cost; it was not retained. That evidence motivates a narrow v7 experiment, rather than establishing a current-v7 bottleneck or a guaranteed benefit.

On ext filesystems, an open v7 runtime supplies a private 64-KiB threshold to the existing dedicated I/O worker. Each DATA write at least that large gets `IOSQE_ASYNC`, asking the kernel to dispatch it asynchronously from the outset. Smaller DATA writes, tmpfs and other formats keep ordinary dispatch. The registered-file flag remains present because the crate's `Entry::flags` adds bits. Ring setup, buffer ownership, completion processing, reservations, batching, FRONTIER write, whole-WAL sync and ACK ordering retain their policies. No worker pause, allocator change or recovery-format change is included.

A real O_DIRECT test writes a 68-KiB forced-async buffer beside an ordinary 4-KiB buffer on a repository-backed filesystem, accepts either completion order, checks full successful completion lengths and every file byte, and closes the worker. The ordinary two-small-write fixture continues to run. Existing v7 extent growth and large-batch recovery tests execute through the modified runtime; worker failure, ambiguity, short-write and ownership tests also pass.

Candidate checks pass **1,973 tests**, zero skipped/retried, with one slow leveled-compaction integration test, plus **47 focused WAL tests**. Formatting, Clippy, dependencies, direct spelling and the release build pass outside the sandbox. The first fixture compile missed a `Path` import; its failed log is preserved in the attempts record, and the import is corrected before successful checks or captures. Restored-source checks pass **1,972 tests**, zero skipped/retried.

## Fixed SSD comparison

The baseline executable SHA-256 is `e9abcc91b499c3cecd0acc137a0c9b8f65228b76ac3cb31ba3894bf9a0d06d40`; candidate is `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8`. The base is `48ae7a7c`. The saved patch reconstructs both changed Rust files, and the manifest binds all 128 inputs.

The unchanged matrix starts with two warmups and five alternating sixteen-writer pairs. Each writes 750,000 entries in batch64, 1-KiB values, explicit v7 Parallel, no compaction and a 2-GiB SST target on fresh local ext4 databases. Initial cooldown is thirty seconds, with ten-second gaps. No build/test/profile/source edit overlaps capture. Before/during 1-Hz/after audits see no recognized competing work; this sampling cannot rule out every short or unrecognized task.

The frozen stall mask flags mean FRONTIER write >=1 ms or mean fdatasync >=1.5 ms; either arm excludes its pair. Both warmups are flagged and never enter scores. Scored pairs1–3 are excluded: all three baselines are flagged, plus candidate pair2. Their apparent speedups do not count toward retention. All raw bytes remain.

| Pair | Baseline / candidate puts/s | Baseline / candidate p99 ms | Filtered comparison |
|---|---:|---:|---|
| 1 | 97,648 / 299,500 | 31.967 / 22.756 | Excluded: baseline stall |
| 2 | 29,267 / 51,969 | 75.070 / 52.276 | Excluded: both stall |
| 3 | 57,663 / 486,508 | 72.642 / 4.777 | Excluded: baseline stall |
| 4 | 437,385 / 508,635 | 18.295 / 4.724 | Included |
| 5 | 414,888 / 456,638 | 16.199 / 8.322 | Included |

The two matched pairs have median paired throughput **1.1318x**, p99 **0.3860x**, sync count **1.0219x** and CPU **1.0024x**; DATA bytes match. Their arm-median throughput ratio is 1.1326x. Throughput/p99 guards pass, but the minimum-pair guard fails. The aggregate mask misses short I/O outliers and cannot make these observations stall-free or prove source causality. In particular, the large p99 gain coincides with longer baseline tails. This is insufficient evidence, rather than an established regression or a qualified speedup.

All five raw scored pairs have median throughput **1.7757x** and p99 **0.5137x**, dominated by the flagged storage variation. No score-selected replacements or further candidates are introduced. Batch8, point8 and batch1 guards are cancelled under the frozen stage rule.

## Final state and evidence

Twelve new invocations remain: ten scored and two warmups. Added to the preceding verified 537, this lineage contains **549 unique invocations**. This screen contains no new v4-parity comparison. The candidate binary and exact patch remain available for a separately planned confirmation.

Both Rust files are restored byte for byte with refreshed modification times before the restored full check. The canonical executable is atomically replaced with the retained baseline, preserving hardlinked artifacts. This slice records evidence only and stops for review.

- [Plan](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-plan.json), [matrix](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-matrix.json), [environment](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-environment.json)
- [Patch](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-candidate.patch), [source manifest](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-source-sha256.tsv), [binaries](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-binaries.json)
- [Raw runs](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-ext4-batch16.jsonl), [commands and hashes](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-ext4-batch16-runs.tsv), [host audits](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-ext4-batch16-host.jsonl)
- [Driver](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-run.py), [stage controller](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-run-all.py), [analyzer](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-analyze.py), [summary](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-summary.json), [scored peaks](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-peaks.json)
- [Checks](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-checks.json), [initial compile](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-attempts.json), [decision](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-decision.json), [verifier](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-verify.py), [verification](artifacts/rfc-025-v7-large-data-async-dispatch-20261027-verification.json)

[WAL documentation](../README.md).
