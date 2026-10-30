# RFC 025: coalesce only eligible v7 prefixes, 2026-10-27

This isolated candidate is **retained for local review**, source commit `d5d23e95`. All four predeclared SSD stages pass. The most useful batch8 comparison observes paired throughput **+9.9%**, p99 **-7.0%** and sync count **-7.2%**, with all three pairs below the aggregate stall thresholds. Batch16 observes **+0.3% throughput / -16.0% p99** across three matched pairs after two flagged pairs are excluded. These observations do not establish uniform gains, v4 parity or RFC adoption.

## Change and validation

The [shared-deadline trial](rfc-025-v7-coalescing-deadline-20261027.md) cut some waits but increased batch8 syncs and lost throughput. This candidate instead preserves the original fresh coalescing budget once a prefix is physically eligible. Before optional coalescing and its capture drain, a healthy v7 written prefix is checked against the allocator's existing `eligible_durable_end_for`. Only an explicit `Ok(None)` skips both optional steps. Queued results remain for the next coordinator iteration, which can use the normal window after eligibility advances.

The regular sync path still performs authoritative validation. Poisoned/idle state and eligibility errors follow the existing path; V4 retains its original coalescing and bounded capture drain. Coalescing durations, ACK rules, codecs and digests stay the same. The System allocator and existing worker threads are kept. No group-geometry or scratch-buffer prototype is included. Source base `d7f5aa8a` retains the earlier accepted f7 optimizations; the shared-deadline and retirement-hint prototypes are not included.

The candidate passes **1,972 tests**, zero skipped/retried, plus **six focused tests**, outside the sandbox. Formatting, Clippy, dependency checks, direct spelling and the release build pass. The new deterministic fixture writes real DATA/FRONTIER bytes. With `D0 | D1 | D2 | F(T=1)`, it queues a successful D2 completion while the written target is still ineligible at T=2. Neither optional coalescing nor its drain may consume it or publish another marker/ACK. The fixture then receives that completion normally, queues later DATA after the old marker, and verifies an eligible coalescing window includes the later group. Close/recovery must return all four batches. Existing recapture and poison cases continue to pass. A test-only helper is split so real completed DATA can be queued without immediately publishing its group result.

## Fixed SSD comparison

The matrix and stall policy are copied unchanged from the preceding trial, with new exact source/executable identities. The baseline is retained `f7fd96e5…`; candidate executable SHA-256 is `e9abcc91b499c3cecd0acc137a0c9b8f65228b76ac3cb31ba3894bf9a0d06d40`. All commands use fresh local ext4 databases, explicit v7 Parallel, no compaction, 1-KiB values and a 2-GiB SST target. Batch64 writes 750,000 entries and samples every ten batches; point8 writes 100,000 puts and samples every hundred. Cooldowns are 30 seconds per cell initially and ten seconds between invocations. No build/test/profile/source edit overlaps capture, and host audits before/during 1 Hz/after see no recognized competing work. Sampling cannot exclude all brief/unrecognized interference.

Batch16 receives two warmups and five alternating pairs. Follow-up batch8, point8 and batch1 each receive two warmups and three alternating pairs. The fixed aggregate mask flags mean FRONTIER writes >=1 ms or mean fdatasync >=1.5 ms; a pair is excluded if either arm is flagged. This implements the request to separate obvious stalls without deleting raw observations. It misses short outliers and DATA/preparation delays and does not prove that remaining runs are stall-free or that I/O variation is independent of source.

The screen requires at least three non-flagged pairs and improved paired/arm-median throughput and paired p99. Follow-ups require at least two pairs, paired/arm-median throughput >=0.95x, p99 <=1.10x and equal DATA bytes. CPU and RSS remain diagnostic, with no CPU veto. All stages complete; no retries, replacements or extra candidates are introduced.

| Workload | Raw paired throughput / p99 | Filtered pairs | Filtered throughput / p99 | Filtered sync count |
|---|---:|---:|---:|---:|
| Batch64, 16 writers | +28.8% / -71.0% | 3 of 5 | +0.3% / -16.0% | -4.6% |
| Batch64, 8 writers | +9.9% / -7.0% | 3 of 3 | +9.9% / -7.0% | -7.2% |
| Point, 8 writers | +86.1% / +2.8% | 3 of 3 | +86.1% / +2.8% | -2.5% |
| Batch64, 1 writer | +46.7% / -8.2% | 3 of 3 | +46.7% / -8.2% | unchanged |

Batch16 excludes pairs2–3 for baseline stalls; neither scored candidate invocation is flagged. In its fastest matching pair, baseline/candidate rates are **534,429/524,397 puts/s**, p99 **4.369/5.093 ms**. The other two matched pairs favor the candidate. A median screen pass is not a uniform sixteen-writer gain.

The complete batch8 scored comparisons are:

| Pair | Baseline puts/s | Candidate puts/s | Baseline / candidate p99 ms | Baseline / candidate syncs |
|---|---:|---:|---:|---:|
| 1 | 294,795 | 293,045 | 11.094 / 8.172 | 1,881 / 1,932 |
| 2 | 273,900 | 306,533 | 8.976 / 8.349 | 1,884 / 1,749 |
| 3 | 260,585 | 286,257 | 8.266 / 8.422 | 2,021 / 1,838 |

All three batch8 pairs remain under the declared aggregate thresholds. Fewer syncs are consistent with preserving a normal batching window after eligibility. This is the grouping behavior the previous shared deadline failed to preserve; individual pair outcomes still vary.

Point throughput's large ratio is storage-confounded. Baseline rates span **2,934–15,715 puts/s**, versus **7,123–16,001** for the candidate. Even though the mean-call thresholds do not flag them, two baseline invocations have p99 near 19 ms. The closest short-I/O pair is **15,715/16,001 puts/s**, p99 **1.280/1.350 ms**. Do not describe this as an established 86% code improvement. Single-writer rates also coincide with varying mean sync times; its measured gain is not isolated from storage variation.

## Highest observed scored throughput

| Workload | Baseline puts/s | Candidate puts/s |
|---|---:|---:|
| ext4-batch16 | 534,429 | 544,147 |
| ext4-batch8 | 294,795 | 306,533 |
| ext4-point8 | 15,715 | 16,001 |
| ext4-batch1 | 27,172 | 44,267 |

These rates are descriptive within each configuration and executable. They do not substitute for paired comparisons or establish v4 parity. All **36 invocations** remain: 28 scored and eight warmups. Twelve of the fourteen scored pairs remain in the filtered view. Adding these runs to the preceding verified 453 yields **489 unique invocations** in this measurement lineage.

## Final state and evidence

Retained source matches the saved 128-input candidate manifest. Canonical `target/release/write-perf` is the candidate executable above. Source and evidence are committed separately; there is no PR, push, format-default adoption or full RFC qualification in this slice.

- [Frozen plan](artifacts/rfc-025-v7-eligible-coalescing-20261027-plan.json), [matrix](artifacts/rfc-025-v7-eligible-coalescing-20261027-matrix.json)
- [Exact patch](artifacts/rfc-025-v7-eligible-coalescing-20261027-candidate.patch), [source manifest](artifacts/rfc-025-v7-eligible-coalescing-20261027-source-sha256.tsv)
- [Executable identities](artifacts/rfc-025-v7-eligible-coalescing-20261027-binaries.json), [environment](artifacts/rfc-025-v7-eligible-coalescing-20261027-environment.json), [checks](artifacts/rfc-025-v7-eligible-coalescing-20261027-checks.json)
- [Batch16](artifacts/rfc-025-v7-eligible-coalescing-20261027-ext4-batch16.jsonl), [batch8](artifacts/rfc-025-v7-eligible-coalescing-20261027-ext4-batch8.jsonl), [point8](artifacts/rfc-025-v7-eligible-coalescing-20261027-ext4-point8.jsonl), [batch1](artifacts/rfc-025-v7-eligible-coalescing-20261027-ext4-batch1.jsonl); each has matching `-runs.tsv` and `-host.jsonl`
- [Analyzer](artifacts/rfc-025-v7-eligible-coalescing-20261027-analyze.py), [summary](artifacts/rfc-025-v7-eligible-coalescing-20261027-summary.json), [peaks](artifacts/rfc-025-v7-eligible-coalescing-20261027-peaks.json), [decision](artifacts/rfc-025-v7-eligible-coalescing-20261027-decision.json)
- [Verifier](artifacts/rfc-025-v7-eligible-coalescing-20261027-verify.py), [verification result](artifacts/rfc-025-v7-eligible-coalescing-20261027-verification.json)

[WAL documentation](../README.md).
