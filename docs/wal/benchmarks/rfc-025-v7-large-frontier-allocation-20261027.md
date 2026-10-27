# RFC 025: allocation-only FRONTIER preparation after large waves, 2026-10-27

This candidate is **not retained**. After excluding two baseline stalls under the frozen aggregate mask, three matched sixteen-writer pairs observe **-2.5% throughput / +10.3% p99**. The fixed screen fails; eight-writer, point-write and single-writer follow-ups are cancelled. The accepted eligible-prefix coalescing source and executable are restored.

## Candidate and correctness

Older allocation traces suggested that FRONTIER preparation sometimes zeroed speculative DATA ranges while extending the ready prefix. Large DATA already uses allocation-only preparation. The candidate therefore lets a FRONTIER use that same path only when the captured durability state already identifies a known large-DATA wave. Small, mixed and unknown waves retain initialization. No new geometry hint or coalescing policy is added.

Both paths retain the shared preparation mutex/cursor and sticky failure handling. A full marker write still completes before the existing WAL sync and ACK publication. Two deterministic ext4 fixtures write a canary into pending DATA beyond the currently written prefix. Large-wave FRONTIER preparation must preserve it; small-wave preparation must zero it. The fixtures then write the actual pending DATA, sync, close and recover both batches exactly. Existing extent growth, paused-worker cursor, poison and failed-marker coverage also runs.

Candidate validation passes **1,974 tests**, zero skipped/retried, plus **20 focused WAL tests**, outside the sandbox. Formatting, Clippy, dependency checks, spelling and the release build pass. The restored source also passes **1,972 tests**, zero skipped; the existing `tests::simple_leveled_compaction::test_integration` times out once and passes its retry. Candidate checks have no retry.

## Fixed SSD screen

Baseline SHA-256 is `e9abcc91b499c3cecd0acc137a0c9b8f65228b76ac3cb31ba3894bf9a0d06d40`; candidate is `7ad88dac2dba0a39b1538b0698afb0d8e3f41deda3f6a0491533a44c1c5d0b3b`. Source base is `95eed41c`. The exact patch and 128-input candidate manifest are preserved even though the candidate is removed from the working source.

The fixed matrix starts with two warmups and five alternating pairs at sixteen writers. Each invocation writes 750,000 entries in batch64, with 1-KiB values, no compaction, a 2-GiB SST target and explicit v7 Parallel. Fresh databases and build artifacts stay in the local repository on ext4. Cooldowns are thirty seconds initially and ten between invocations. No build/test/profile/source edit overlaps capture. Before/during 1-Hz/after host audits observe no recognized competing work; this sampling cannot exclude every brief or unrecognized interference.

As requested, obvious stalls do not drive retention. The unchanged predeclared mask flags mean FRONTIER write >=1 ms or mean fdatasync >=1.5 ms and excludes a pair when either arm is flagged. Both warmups stall; warmups never enter scores. Scored baseline pairs2–3 are flagged, with mean marker/sync times of 5.635/4.357 ms and 3.076/2.624 ms. Their large apparent candidate gains are excluded. Every raw observation remains available.

| Matched pair | Baseline puts/s | Candidate puts/s | Baseline / candidate p99 ms | Baseline / candidate syncs |
|---|---:|---:|---:|---:|
| 1 | 495,046 | 482,765 | 4.805 / 5.299 | 1,326 / 1,338 |
| 4 | 547,658 | 502,335 | 5.090 / 4.981 | 1,261 / 1,311 |
| 5 | 388,848 | 394,336 | 14.798 / 18.780 | 1,262 / 1,337 |

Median paired throughput is **0.9752x**, p99 **1.1028x**, and sync count **1.0397x**. The arm-median throughput ratio is also 0.9752x; DATA bytes match in every pair. All three filtered screen benefit guards fail despite enough matched pairs. CPU rises 5.3% in this view and remains diagnostic rather than a veto. Across all five raw scored pairs, the median is +1.4% throughput / -2.1% p99, demonstrating why the flagged stalls must be separated.

The aggregate mask misses short outliers: pair5 still has long p99 tails. These three pairs do not isolate a universal source-induced slowdown. They also provide no basis for retaining this candidate: both lower-tail matched pairs lose throughput, and the fixed screen requires better throughput and p99. Packer preallocation time falls 9.3% in the matched median, but FRONTIER write time rises 13.4% and sync count rises 4.0%. Those counters are consistent with work shifting into marker I/O and grouping variation; they do not measure total zero bytes or prove causation.

## Final state and evidence

There are twelve new invocations: ten scored and two warmups. No replacement runs, extra candidates or dependent stages are added after the screen miss. Combined with the prior verified 525, this lineage reaches **537 unique invocations**. There is no new v4 parity result in this screen.

The exact retained source bytes are restored with a fresh modification time to force correct recompilation. Canonical `target/release/write-perf` is atomically replaced from the retained executable, preserving any hardlinked build artifact. The verifier reconstructs all candidate inputs from the base plus saved patch, checks the restored inputs/executables, regenerates the raw and filtered summary, validates invocation uniqueness and binds all check logs. This slice commits evidence only.

- [Frozen plan](artifacts/rfc-025-v7-large-frontier-allocation-20261027-plan.json), [matrix](artifacts/rfc-025-v7-large-frontier-allocation-20261027-matrix.json), [environment](artifacts/rfc-025-v7-large-frontier-allocation-20261027-environment.json)
- [Candidate patch](artifacts/rfc-025-v7-large-frontier-allocation-20261027-candidate.patch), [source manifest](artifacts/rfc-025-v7-large-frontier-allocation-20261027-source-sha256.tsv), [executable identities](artifacts/rfc-025-v7-large-frontier-allocation-20261027-binaries.json)
- [Raw captures](artifacts/rfc-025-v7-large-frontier-allocation-20261027-ext4-batch16.jsonl), [commands and hashes](artifacts/rfc-025-v7-large-frontier-allocation-20261027-ext4-batch16-runs.tsv), [host audits](artifacts/rfc-025-v7-large-frontier-allocation-20261027-ext4-batch16-host.jsonl)
- [Driver](artifacts/rfc-025-v7-large-frontier-allocation-20261027-run.py), [stage controller](artifacts/rfc-025-v7-large-frontier-allocation-20261027-run-all.py), [analyzer](artifacts/rfc-025-v7-large-frontier-allocation-20261027-analyze.py), [summary](artifacts/rfc-025-v7-large-frontier-allocation-20261027-summary.json), [raw scored peaks](artifacts/rfc-025-v7-large-frontier-allocation-20261027-peaks.json)
- [Checks](artifacts/rfc-025-v7-large-frontier-allocation-20261027-checks.json), [decision](artifacts/rfc-025-v7-large-frontier-allocation-20261027-decision.json), [verifier](artifacts/rfc-025-v7-large-frontier-allocation-20261027-verify.py), [verification](artifacts/rfc-025-v7-large-frontier-allocation-20261027-verification.json)

[WAL documentation](../README.md).
