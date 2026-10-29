# RFC 025: ordinary completion notification, independent SSD retry

The maintainer requested a retry of the [ordinary-notification candidate](rfc-025-v7-signal-taskwork-20261029.md).
It remains **inconclusive and is not retained**. The missing 16-writer batch guard
gets five fresh alternating pairs; only two survive the unchanged stall cutoff.
Their median paired throughput falls 6.7% and p99 falls 2.3%. This supplies neither
the required three matched pairs nor a passing throughput guard. The dependent
point-write confirmation and four other guards are cancelled as planned.

All 12 enabled invocations finish: two warmups and ten scored runs. No replacement
pairs or additional candidates are tried, and no earlier captures are pooled into
this cohort. The 128 retained build inputs and canonical release executable are
restored exactly. V4 parity and the earlier forced-async dispatch qualification
remain open.

The candidate is byte-identical to the previous trial: active ext-filesystem v7
workers request `SINGLE_ISSUER` with ordinary notification; other workers retain
`COOP_TASKRUN | SINGLE_ISSUER`. The existing `EINVAL` fallback, large-DATA async
dispatch, submission, ownership, completion handling, and one-sync durability
protocol remain as in that trial. There is no new runtime policy in this retry.

The saved binaries are reused after fresh source/hash and correctness validation:

| Arm | Saved binary | SHA-256 |
| --- | --- | --- |
| Retained | `target/release/write-perf-v7-large-data-async-dispatch` | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Ordinary notification | `target/release/write-perf-v7-signal-taskwork` | `79592a3cbf0c4f9e5411690cd5e369e62b9abdc6991f88b03b0b0d9b42071889` |

The [frozen plan](artifacts/rfc-025-v7-signal-taskwork-retry-20261029-plan.json)
puts batch16 first, with two warmups and five alternating pairs. It requires at
least three matched pairs, paired and arm-median throughput ratios of at least
0.95, and paired p99 of at most 1.10. A passing first stage would enable a fresh
five-pair point8 confirmation and three-pair point1, point16, batch8 and batch1
guards. The point8 confirmation requires both throughput measures to improve and
p99 to improve, with at least three matched pairs. Every enabled followup would
finish regardless of interim performance. Maximum cohort size is 56 invocations.

The completed cell uses fresh local ext4 databases on Linux `6.12.71-1-lts`,
16 writers, batch64, 750,000 puts, 1 KiB values, a 2 GiB SST target, no compaction,
explicit v7 Parallel and latency sampling every ten batches. Each arm supplies
1,184 latency samples. Cooldown is 60 seconds before the cell and 15 seconds
between invocations. There are no overlapping builds, tests, profiles, source
edits or SSD health queries. Before/1 Hz/after host audits find no recognized
competing process; sampling cannot establish exclusive device access.

The cutoff is unchanged: exclude a whole pair if either arm's mean FRONTIER write
is at least 1 ms or its mean `fdatasync` is at least 1.5 ms. All raw evidence stays
available. This aggregate rule misses short outliers, does not prove the retained
pairs stall-free, and cannot distinguish source-induced I/O changes from storage
variation.

| Pair | Retained puts/s | Candidate puts/s | Retained p99 ms | Candidate p99 ms | Matched view |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | 542,931 | 540,956 | 4.720 | 4.260 | Include |
| 2 | 99,870 | 105,777 | 28.764 | 26.423 | Exclude: both FRONTIER means exceed cutoff |
| 3 | 95,597 | 94,679 | 33.447 | 29.773 | Exclude: both FRONTIER means exceed cutoff |
| 4 | 546,281 | 146,180 | 4.847 | 21.826 | Exclude: candidate FRONTIER mean exceeds cutoff |
| 5 | 534,057 | 464,579 | 5.309 | 5.578 | Include |

| Pair | Retained FRONTIER mean ms | Candidate FRONTIER mean ms | Retained sync mean ms | Candidate sync mean ms |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.104 | 0.063 | 0.354 | 0.357 |
| 2 | 1.644 | 1.516 | 1.194 | 1.165 |
| 3 | 1.738 | 1.488 | 1.076 | 1.276 |
| 4 | 0.068 | 1.121 | 0.351 | 0.775 |
| 5 | 0.070 | 0.114 | 0.392 | 0.388 |

The two matched pairs give a paired throughput ratio of 0.93313, an arm-median
throughput ratio of 0.93365 and a paired p99 ratio of 0.97656. CPU is 9.2% higher
by paired median and remains diagnostic. Every scored arm writes exactly
816,054,272 DATA bytes. Pair 1 has 0.4% lower throughput and 9.7% lower p99;
pair 5 has 13.0% lower throughput and 5.1% higher p99. A repeatable benefit is
not established. These two pairs also do not establish a general source-caused
regression.

The all-pair median ratios, including excluded stalls, are 0.99039 for throughput
and 0.91860 for p99. They do not override the predeclared matched view. Arm peaks
are preserved as descriptive evidence in the
[peak manifest](artifacts/rfc-025-v7-signal-taskwork-retry-20261029-peaks.json).
No independently selected peak pair is used to qualify this candidate.

Fresh outside-sandbox validation passes 49 focused tests without retries and
1,975 tests in `cargo make check`, including the mixed and small-only real-I/O
worker cases. No selected I/O path is skipped. The exact earlier release binary
is used, so there is no new release build or profiler in this cohort. After
restoration, a fresh `cargo make check` passes all 1,973 tests without retries or
skips. Formatting, strict clippy, dependency checks and spelling pass.

Reproduce analysis and verification from the repository root:

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-signal-taskwork-retry-20261029-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-signal-taskwork-retry-20261029-verify.py
```

The verifier reconstructs all 128 candidate inputs from `b4001ad2` plus the saved
patch, checks identity against the previous candidate and its build provenance,
regenerates the summary, validates the fixed schedule, audits, hashes and
cancellation, and verifies exact retained-source/executable restoration. The
earlier verification receipt was reproduced before reapplying the candidate and
is preserved unchanged. This cohort adds 12 unique completed invocations to the
857-invocation lineage, reaching **869**, with 4,965 unique invocations in the
broader archive and one inherited interrupted invocation without JSON. Scratch
databases are removed; logs and saved binaries stay under the local repository's
ignored `target` directory. No push or PR is made.
