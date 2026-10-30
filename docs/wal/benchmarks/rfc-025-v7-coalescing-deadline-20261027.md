# RFC 025: one coalescing deadline across v7 retries, 2026-10-27

This candidate is **not retained**. The predeclared stall-filtered sixteen-writer screen improves median paired throughput **0.8%** and p99 **14.3%**, but the eight-writer follow-up loses **11.8% throughput** with **1.3% higher p99** and **11.1% more syncs**. None of its three pairs meets the declared obvious-stall rule. The kept f7 source/executable is restored; point8 and batch1 checks are cancelled. CPU remains diagnostic and is not a rejection criterion.

## Candidate and correctness

The [previous investigation](rfc-025-v7-16-writer-tail-diagnosis-20261027.md) finds repeated optional waits after a completed prefix cannot retain its preceding FRONTIER without also retaining excluded DATA. This candidate keeps one coordinator-local deadline keyed by the current durable ticket frontier. A healthy contiguous written prefix starts it; further attempts at the same durable boundary receive only the remaining budget. A durability advance permits a fresh window. Idle/out-of-order-only state does not start it. V4 continues to use a fresh deadline.

Only `wal/parallel/runtime.rs` changes. Existing 100/400/800-us policy selection, DATA/FRONTIER encoding, prefix eligibility, poisoning, reservation ownership and sync-before-ACK remain. The earlier retirement-hint and FRONTIER-buffer-reuse prototypes are not included. This isolates deadline reuse against retained `f7fd96e5…`, source base `cda2ce87`.

All **1,972 tests** pass, zero skipped/retried, plus six focused tests, outside the sandbox. Formatting, Clippy, dependency checks, direct spelling and the release build pass. The new deterministic fixture writes real DATA and FRONTIER bytes, produces a physically ineligible prefix, verifies no extra marker or ACK, and simulates an expired deadline without sleeping. A queued error result must remain for the normal capture drain rather than being consumed by a renewed wait. A subsequent successful sync resets the deadline scope; close/recovery must return all five batches. Existing recapture and poison tests also pass.

## Frozen comparison and stall handling

The plan captures two warmups and five alternating baseline/candidate pairs at sixteen writers. If that screen improves throughput and p99 on at least three non-flagged matched pairs, batch8, point8 and batch1 each receive two warmups plus three alternating pairs. Follow-ups require at least two non-flagged pairs, paired and separate-arm median throughput at least 0.95x, paired p99 at most 1.10x, and equal DATA bytes. No slow run is retried, replaced or removed from the raw artifacts.

All runs use fresh local ext4 databases, explicit v7 Parallel, no compaction, 1-KiB values and a 2-GiB SST target. Batch64 writes 750,000 entries, sampling every ten batches. The cancelled point8 case would write 100,000 entries and sample every hundred puts. There is a 30-second initial cooldown per cell and ten seconds between invocations. Host audits before, during at 1 Hz, and after each command see no recognized competing work. No build/test/profile/source edit overlaps captures.

To implement the maintainer's request to ignore obvious stalls, the plan flags an invocation if **mean FRONTIER write >=1 ms or mean fdatasync >=1.5 ms**. A pair is excluded from the filtered view if either arm is flagged. Thresholds are fixed before coding/capture, relative to observed quiet marker writes around 20–150 us and syncs around 350 us. This aggregate rule misses short outliers and cannot prove that unflagged samples are stall-free or distinguish every source-induced I/O change from environment variation. All raw pairs, masks and arm-specific exclusion counts remain visible.

## Results

| Workload | Raw paired throughput / p99 | Filtered pairs | Filtered throughput / p99 | Filtered sync count |
|---|---:|---:|---:|---:|
| Batch64, 16 writers | -4.7% / +17.1% | 3 of 5 | +0.8% / -14.3% | +2.7% |
| Batch64, 8 writers | -11.8% / +1.3% | 3 of 3 | -11.8% / +1.3% | +11.1% |

The sixteen-writer mask excludes pairs1–2: candidate mean marker writes reach 4.95 and 9.23 ms, with mean syncs 3.75 and 6.29 ms; pair2's baseline is also flagged. Its three retained pairs are not uniformly favorable: the fastest matching pair reaches **552,160 versus 526,166 puts/s**, p99 **4.286 versus 5.021 ms**. The other two pairs favor the candidate. Passing this median screen does not establish a uniform sixteen-writer improvement.

The complete eight-writer scored results are:

| Pair | Baseline puts/s | Candidate puts/s | Baseline / candidate p99 ms | Baseline / candidate syncs |
|---|---:|---:|---:|---:|
| 1 | 305,772 | 265,979 | 9.722 / 8.333 | 1,762 / 1,957 |
| 2 | 315,261 | 277,974 | 8.597 / 8.710 | 1,601 / 1,832 |
| 3 | 262,214 | 249,755 | 8.306 / 8.748 | 1,872 / 2,011 |

Candidate throughput loses in every eight-writer pair. Mean marker writes stay within **104–161 us** and syncs within **407–484 us** across both arms. The extra cycles also increase aggregate coalescing time even though an individual retry cannot restart its deadline. Removing repeated waits permits earlier smaller captures; the additional syncs are consistent with reduced grouping efficiency. These measurements do not isolate every scheduling or storage contribution, but an obvious-stall-only explanation is not supported by the declared mask.

All **20 invocations** remain: sixteen scored and four warmups. Raw scored peaks are 552,160/526,166 baseline/candidate puts/s at sixteen writers and 315,261/277,974 at eight. Peaks are descriptive and never replace paired comparisons. The prior verified 433 invocations plus these twenty give **453 unique invocations** in this measurement lineage.

## Final state and evidence

The restored full check passes **1,971 tests**, with one unrelated `simple_leveled_compaction::test_integration` timeout passing on retry. Source mtimes are refreshed so Cargo recompiles the retained bytes. Canonical `target/release/write-perf` again has SHA-256 `f7fd96e5ca0ecaaaa23f59f135e7ce9430521f986834e5c95f2d32070e378d54`. The candidate patch, binary and 128-input manifest remain available for review. No new source optimization, allocator/runtime change, PR, push or RFC performance qualification is included.

- [Frozen plan](artifacts/rfc-025-v7-coalescing-deadline-20261027-plan.json)
- [Fixed matrix](artifacts/rfc-025-v7-coalescing-deadline-20261027-matrix.json)
- [Exact candidate patch](artifacts/rfc-025-v7-coalescing-deadline-20261027-candidate.patch)
- [Source manifest](artifacts/rfc-025-v7-coalescing-deadline-20261027-source-sha256.tsv), [binary identities](artifacts/rfc-025-v7-coalescing-deadline-20261027-binaries.json), [environment](artifacts/rfc-025-v7-coalescing-deadline-20261027-environment.json)
- [Checks](artifacts/rfc-025-v7-coalescing-deadline-20261027-checks.json), [restoration](artifacts/rfc-025-v7-coalescing-deadline-20261027-restoration.json)
- [Batch16 raw](artifacts/rfc-025-v7-coalescing-deadline-20261027-ext4-batch16.jsonl), [batch8 raw](artifacts/rfc-025-v7-coalescing-deadline-20261027-ext4-batch8.jsonl); each has matching `-runs.tsv` and `-host.jsonl`
- [Analyzer](artifacts/rfc-025-v7-coalescing-deadline-20261027-analyze.py), [summary](artifacts/rfc-025-v7-coalescing-deadline-20261027-summary.json), [peaks](artifacts/rfc-025-v7-coalescing-deadline-20261027-peaks.json), [decision](artifacts/rfc-025-v7-coalescing-deadline-20261027-decision.json)
- [Verifier](artifacts/rfc-025-v7-coalescing-deadline-20261027-verify.py), [verification result](artifacts/rfc-025-v7-coalescing-deadline-20261027-verification.json)

[WAL documentation](../README.md).
