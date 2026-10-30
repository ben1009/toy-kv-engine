# RFC 025: reuse the coordinator FRONTIER buffer, 2026-10-27

The buffer-reuse candidate is **not retained**. The fixed comparison stopped at batch16, with large storage variation also visible in the unchanged v4 controls. The fastest matched v7 batch16 pair is effectively tied. This evidence does not establish that buffer reuse causes a 16-writer regression, or that it provides a repeatable SSD benefit. The retained `f7fd96e5…` source and executable are restored. [The follow-up investigation](rfc-025-v7-16-writer-tail-diagnosis-20261027.md) separates obvious stalls and examines the earlier retirement-hint prototype.

The retained coordinator allocates and frees an aligned 4-KiB `DirectBuf` for each FRONTIER. The earlier excluded retained-binary profiles observe 11,719 / 1,662 / 1,315 FRONTIER writes at one / eight / sixteen writers. Allocation is on the commit path; these observations do not establish its share of blocked SSD time or predict a throughput gain. This bounded candidate tests removing that repeated allocation without another coalescing policy.

The sync coordinator owns an initially empty optional frame. The first eligible v7 cycle allocates it; later cycles reuse it after the synchronous FRONTIER write and WAL sync finish. Encoding still replaces the complete header, body, padding and checksums. V4 and idle coordinators allocate no FRONTIER scratch. Error/poison exits drop the same owned allocation; no asynchronous I/O points at the scratch after the synchronous call returns. The existing DATA ownership, marker credit, byte format, SHA/CRC domains, captured prefix and one-sync-before-ACK rules remain. There are no new queues, per-group fields, runtime or allocator switches, or deadline changes.

## Validation

The candidate passes **1,971 tests**, zero skipped/retried, plus **five focused tests** with immediate output outside the sandbox. Formatting, Clippy, dependency checks, direct spelling and the release build pass. The exact build inputs and logs are recorded below. The existing wave test is strengthened; the total test count remains unchanged.

The real multi-cycle prefix fixture starts with no written DATA and verifies no allocation occurs. It reuses one aligned address across successful cycles, deliberately dirties the prior frame between them, and verifies no-op sync preserves the allocation. Exact recovery of mixed small/large batches then validates the rewritten frame bytes. Existing short-marker, failed-sync, recapture, poison and close tests remain relevant. Tests and release builds run outside the sandbox before scoring; no source edit/build/test/profile overlaps measured commands.

## Fixed SSD comparison

The original `f7fd96e5…` executable and retained source are the baseline. One source candidate is evaluated. The three earlier user-CPU profiles are reused and excluded, counted once across investigations. Their default-leveled configuration differs from the explicitly no-compaction score runs; samples also include setup/close. SHA leaf samples appear mainly on the ordered-packer thread, but no complete call chain or off-CPU attribution is claimed.

The frozen order is batch8, batch16, batch1, point8, and independent batch8 confirmation. Every completed cell has four warmups and three alternating four-arm rounds: baseline/candidate v4/v7. All databases are fresh local ext4 SSD images, with 1-KiB values and a 2-GiB SST target. Batch64 writes 750,000 puts, sampling every ten batches; point8 writes 100,000, sampling every 100 puts. Cooldowns are 30 seconds initially and ten seconds between invocations. Host audits check recognized competing work before/during 1 Hz/after; sampling cannot exclude every brief/unrecognized task or SSD stall.

A stage requires paired and separate-arm median v7 throughput >=0.95x, paired p99 <=1.10x and identical DATA byte counts. Independent batch8 confirmation must improve throughput >1.00x and p99 <1.00x. CPU, RSS, sync counts and v4 controls remain diagnostic, with no CPU veto. A failed stage cancels dependents. No slow run is filtered or replaced, and peaks never substitute for paired comparisons.

## Results

| Cell | Paired throughput | Paired p99 | V7 raw guards | V4 throughput / p99 control |
|---|---:|---:|---|---:|
| Batch64, 8 writers | +2.1% | -15.4% | Pass | 0.221x / 4.043x |
| Batch64, 16 writers | -36.8% | +58.2% | Fail | 3.642x / 0.122x |

These are medians of paired ratios, not filtered scores. The fast final 16-writer pair is 577,743 versus 576,255 puts/s, with p99 4.392 versus 4.337 ms. In contrast, one slow candidate invocation falls to 77,998 puts/s while mean FRONTIER write time rises from about 63 us to 3.627 ms; sync count changes only from 1,214 to 1,230. Treating that observation as a proven CPU/source regression would be unsupported.

All **32 invocations** remain: 24 scored and eight warmups. The batch1, point8 and independent batch8 confirmation stages were cancelled by the frozen controller. The maintainer subsequently asked to disregard obvious stalls when diagnosing source effects; filtered diagnostic views preserve their masks and raw observations in the follow-up report. They do not replace this original capture.

## Highest observed scored throughput

| Cell | Executable / format | Highest puts/s | Run ID |
|---|---|---:|---|
| ext4-batch8 | old_v4 | 393,932 | `1793085056779` |
| ext4-batch8 | new_v4 | 391,624 | `1793084949429` |
| ext4-batch8 | old_v7 | 289,279 | `1793084963696` |
| ext4-batch8 | new_v7 | 386,416 | `1793085159907` |
| ext4-batch16 | old_v4 | 155,598 | `1793085415032` |
| ext4-batch16 | new_v4 | 566,639 | `1793085401437` |
| ext4-batch16 | old_v7 | 577,743 | `1793085479725` |
| ext4-batch16 | new_v7 | 576,255 | `1793085491245` |

Peak rates are descriptive within matched configurations. Failed controls and storage variation remain part of the evidence; this slice cannot establish v4 parity or full RFC adoption.

## Final state

Retained runtime and worker bytes match the original source base, and canonical `target/release/write-perf` again has SHA-256 `f7fd96e5ca0ecaaaa23f59f135e7ce9430521f986834e5c95f2d32070e378d54`. The restored full check passes 1,971 tests, with one unrelated compaction test passing on retry. Source mtimes were refreshed so later checks cannot silently reuse a candidate test artifact.

The patch, executable and input manifest preserve the rejected candidate. The follow-up adds 17 excluded diagnostics across distinct instrumentation stages; they are counted separately from these 32 invocations and from the preceding 384. No PR, push, default adoption, or RFC performance qualification is included.

## Evidence

- [Frozen plan](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-plan.json)
- [Ordered matrix](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-matrix.json)
- [Exact candidate patch](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-candidate.patch)
- [128-input source manifest](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-source-sha256.tsv)
- [Executable identities](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-binaries.json)
- [Environment](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-environment.json)
- [Checks](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-checks.json)
- [Stage controller](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-run-all.py)
- [Summary](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-summary.json)
- [Highest scores](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-peaks.json)
- [Decision](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-decision.json)
- [Verifier](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-verify.py)
- [Verification result](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-verification.json)

Raw cells: [batch8](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-ext4-batch8.jsonl), [batch16](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-ext4-batch16.jsonl). Each has a matching `-runs.tsv` and `-host.jsonl`. [Restoration](artifacts/rfc-025-v7-frontier-buffer-reuse-20261027-restoration.json).

[WAL documentation](../README.md).
