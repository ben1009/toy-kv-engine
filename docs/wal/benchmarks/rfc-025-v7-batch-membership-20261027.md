# RFC 025: minimum-write geometry for v7 batch coalescing, 2026-10-27

**Result: reject this first source prototype. The initial batch8 result improves, but batch16 throughput/p99, point8 throughput and independent batch8 confirmation fail. The kept f7 source/executable is restored before a separately frozen corrective prototype. CPU is diagnostic and does not determine rejection.**

This experiment starts from `dc3e3c1a`, retaining the provisionally kept batch-window source `d7fbe1c9` and executable `f7fd96e5…`. It targets a batch-specific blind spot without extending the coalescing deadline again. System, SkipMap, asynchronous extent preparation and the serial hybrid remain selected.

## Diagnosis before source changes

Three fresh retained SSD profiles cover batch64 at one, eight and sixteen writers. They record 583 / 254 / 236 `cpu-clock:u` samples at 99 Hz with DWARF stacks and zero lost samples. Sampling covers the whole child, including setup/close; engine counters cover the measured write interval. The profiles use the default leveled configuration, while scored runs explicitly disable compaction. Their 750,000 puts remain below the 2-GiB SST target. Profile timing is diagnostic, never part of scoring or a substitute for matched comparisons.

At eight writers, 11,720 logical batches produce 7,617 DATA groups, including only 4,299 single-ticket groups. Thus **63.3% of logical batches are in multi-ticket groups**. At sixteen writers, 11,728 batches produce 6,212 groups with 3,102 single-ticket groups: **73.6% of batches are in multi-ticket groups**. At one writer all 11,719 groups have one ticket. These fractions include final partial batches, whose smaller geometry must stay conservative.

The retained hint recognizes a large group only when it has one ticket and at least 64 KiB of write bytes. Any multi-ticket group disables the longer wait for its unsynced wave, even when every logical batch is large. The eight/sixteen-writer diagnostic captures cover 7.05 / 8.92 batches per sync, respectively. The hypothesis is that recognizing known large multi-ticket groups can share a FRONTIER and sync across more batches. It is not a guarantee that a longer eligible wait increases throughput or lowers p99.

SHA-256 occupies about 20–23% of the whole-child user samples. This does not isolate the timed hash stage or kernel/off-CPU work, and the candidate does not remove required hashing. Existing diagnostic FRONTIER/sync/preparation timers overlap and have different ownership; they cannot be summed into wall time or compared as if they were scored results.

## Source change

`WriteGroup` already validates every DATA write length and offset while checking its allocation span. That loop now also captures `min_write_bytes`. The minimum passes unchanged through worker group ownership, completion results and failure snapshots. Unsubmitted/rejected failures carry zero to mean unavailable geometry. Reserved FRONTIER gaps do not contribute to the DATA minimum.

The coordinator classifies a group as all-large only when its nonzero DATA write count equals its logical ticket count and its minimum write length is at least 64 KiB. V7 materializes one contiguous DATA write per logical batch, so this proves every batch is large. A missing minimum or mismatched count stays conservative. Mixed groups cannot qualify merely because a huge batch raises their average size.

The cached hint still folds only as contiguous completions advance, resets after a durable cutoff and cannot advance durability. Wait selection remains 800 microseconds after a slow sync for large v7 waves, 400 for ordinary slow-sync waves, and 100 after fast v7 syncs. Catching admission returns immediately; one writer pays no compulsory grace. Point/v4 selection, bounded completion capture, encoded bytes, persisted hashes, FRONTIER chain, reservations, ownership, poison and sync-before-ACK rules retain their behavior. No new admission-map lookup or lock is added.

This differs from earlier rejected group-hint lookup and cheap-sync-skip policies: the candidate propagates existing write geometry through the I/O worker and retains the currently kept waiting policy. It does not repeat an admission-side membership scan or alter the fast-sync decision.

## Correctness

The new source passes **1,973 tests** outside the sandbox, with zero skips/retries and one unrelated slow leveled-compaction test. Eight focused tests run with immediate output. A worker test verifies minimum DATA size through a reserved FRONTIER gap, reversed CQEs, differently sized writes, larger buffer capacity, short-write failure, failure snapshots and exact buffer retirement. A real v7 worker test admits two logical batches as one I/O group and verifies both all-large and huge-plus-tiny mixed membership, durable publication, accounting, close and exact recovery. Existing tests cover unavailable geometry, out-of-order prefix folding, poison and recapture.

Formatting, Clippy, dependency checks and release build pass. The make task reports a joined-word spelling error in the unscored plan but returns zero; the metadata is corrected before scoring and a direct repository `typos` command passes. The original full-check log is preserved, and the separate spelling result prevents treating its exit status alone as proof of a clean spelling check. No Rust source changes occur after the successful checks/build.

## Fixed SSD comparison

Five cells run regardless of performance outcomes: batch64 at 1/8/16 writers, point8, and independent batch8 confirmation. Every cell has one warmup per arm and three alternating four-arm rounds: retained v4, candidate v4, retained v7 and candidate v7; even rounds reverse the order. All use fresh ext4 SSD databases, 1-KiB values and a 2-GiB SST target. Batch64 writes 750,000 puts and samples every ten batches; point8 writes 100,000 puts and samples every 100 operations. Sample counts are recomputed from per-thread remainders. A 30-second initial cooldown and ten-second intercommand gaps are fixed before scoring.

V7 provisional retention requires paired and separate-arm median throughput at least 0.95x and paired p99 at most 1.10x in every cell, identical DATA byte counts, and independent batch8 confirmation with throughput above 1.00x and p99 below 1.00x. **CPU is diagnostic, without a retention veto**, following the maintainer's preference for throughput/p99 benefit. RSS, sync counts, v4 controls and v7/v4 normalization remain visible. No score-selected retries or RAM gate is used.

No build, test or profile overlaps scoring. Recognized competing processes are sampled before, once per second during, and after commands; this does not rule out every brief or unrelated task. Storage stalls and v4 post-measurement SST/drain work can influence later device state despite the cooldown. Every completed observation and failed control stays in the record. CPU is measured process user plus system time for the writes; exact-child wait4 peak RSS includes setup and close. Peak results are descriptive and never replace paired estimates.

## Results

### V7

| Workload / writers | Retained puts/s | Candidate puts/s | Paired throughput | p99 | CPU | Syncs | Guards |
|---|---:|---:|---:|---:|---:|---:|---|
| batch1 | 27,382 | 29,976 | +29.5% | -6.0% | -6.7% | +0.0% | Pass |
| batch8 | 77,450 | 223,089 | +188.0% | -7.0% | -43.9% | -21.5% | Pass |
| batch16 | 527,454 | 442,851 | -14.0% | +20.2% | +8.1% | -7.6% | Fail |
| point8 | 14,133 | 5,117 | -12.4% | +0.1% | +4.7% | +1.6% | Fail |
| confirmation batch8 | 354,023 | 99,886 | -51.4% | +309.7% | +50.9% | +14.9% | Fail |

### V4 controls

| Workload / writers | Retained puts/s | Candidate puts/s | Paired throughput | p99 | CPU | Syncs | Guards |
|---|---:|---:|---:|---:|---:|---:|---|
| batch1 | 29,565 | 25,405 | -6.9% | +3.8% | +1.6% | +0.0% | Fail |
| batch8 | 132,096 | 86,943 | +0.6% | +3.6% | +2.4% | -1.6% | Fail |
| batch16 | 554,150 | 49,898 | -91.0% | +1703.5% | +22.6% | -8.8% | Fail |
| point8 | 5,569 | 12,467 | +2.0% | -4.6% | -6.2% | +0.2% | Pass |
| confirmation batch8 | 87,138 | 307,187 | +254.1% | -16.0% | -10.8% | +1.0% | Pass |

The initial batch8 estimate is heavily affected by slow baseline runs. The independent batch8 cohort reverses direction: throughput -51.4%, p99 4.10x and sync count +14.9%. The failed confirmation is preserved. Batch16 and point8 also fail; no small CPU increase is used as a veto. V4 controls vary substantially, including a 16-writer paired throughput ratio 0.090x, so not every timing difference is isolated as a source effect.

A fast 16-writer pair nevertheless exposes a concrete cost: sync count falls from 1,295 to 1,181, while aggregate coalescing wait rises from 572.5 to 740.1 ms. Throughput falls from 527,454 to 442,851 puts/s and p99 rises from 4.676 to 5.619 ms. These counters are consistent with too much waiting after modest-cost syncs; they do not prove every slowdown has that cause. The extra 64-bit completion field also expands the 64-byte message to 72 bytes. The [corrective prototype](rfc-025-v7-batch-wait-budget-20261027.md) tests compact metadata and a cost limit on additional waiting as a distinct source candidate.


## Highest scores within this cohort

| Workload | Retained v4 | Candidate v4 | Retained v7 | Candidate v7 |
|---|---:|---:|---:|---:|
| batch1 | 78,482 | 75,268 | 67,062 | 63,023 |
| batch8 | 185,779 | 217,031 | 124,770 | 374,780 |
| batch16 | 559,204 | 113,253 | 534,373 | 459,345 |
| point8 | 16,856 | 13,692 | 15,281 | 12,381 |
| confirmation batch8 | 343,065 | 381,059 | 380,797 | 185,165 |

Peak IDs, raw hashes and executable hashes are recorded for every arm. Historical peaks and excluded profiles are not substituted into the fixed comparisons.

## Final state

All 80 measured/warmup invocations remain: 60 scored and 20 warmups, plus three excluded diagnostic invocations. The exact rejected source, saved 0062d35b… executable and patch remain available. The baseline restoration is historical: it records the actual restored kept f7 state before the separately planned corrective prototype. The latter owns final live-source/executable verification. No source commit or performance qualification is attributed to this failed variant.

The investigation extends beyond its original single-candidate bound to correct the avoidable costs exposed by validation. The first result stays failed, with no replacement, pooling or silent guard override. Final spelling and artifact verification are performed after the bounded three-variant investigation finishes.

## Evidence

- [Frozen plan](artifacts/rfc-025-v7-batch-membership-20261027-plan.json)
- [Fresh diagnostic plan](artifacts/rfc-025-v7-batch-membership-20261027-diagnostic-plan.json)
- [Profile metadata](artifacts/rfc-025-v7-batch-membership-20261027-profiles.json)
- [Raw diagnostic records](artifacts/rfc-025-v7-batch-membership-20261027-profiles.jsonl)
- [Computed group diagnosis](artifacts/rfc-025-v7-batch-membership-20261027-diagnosis.json)
- [Fixed matrix](artifacts/rfc-025-v7-batch-membership-20261027-matrix.json)
- [Exact patch](artifacts/rfc-025-v7-batch-membership-20261027-candidate.patch)
- [128-input source manifest](artifacts/rfc-025-v7-batch-membership-20261027-source-sha256.tsv)
- [Saved executable identities](artifacts/rfc-025-v7-batch-membership-20261027-binaries.json)
- [Environment](artifacts/rfc-025-v7-batch-membership-20261027-environment.json)
- [Checks](artifacts/rfc-025-v7-batch-membership-20261027-checks.json)
- [All-cell driver](artifacts/rfc-025-v7-batch-membership-20261027-run-all.py)
- [Summary](artifacts/rfc-025-v7-batch-membership-20261027-summary.json)
- [Highest scores](artifacts/rfc-025-v7-batch-membership-20261027-peaks.json)
- [Decision](artifacts/rfc-025-v7-batch-membership-20261027-decision.json)
- [Verifier](artifacts/rfc-025-v7-batch-membership-20261027-verify.py)
- [Verification result](artifacts/rfc-025-v7-batch-membership-20261027-verification.json)

| Cell | Raw records | Commands/identities | Process audit |
|---|---|---|---|
| ext4-batch1 | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch1.jsonl) | [TSV](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch1-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch1-host.jsonl) |
| ext4-batch8 | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch8.jsonl) | [TSV](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch8-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch8-host.jsonl) |
| ext4-batch16 | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch16.jsonl) | [TSV](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch16-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-ext4-batch16-host.jsonl) |
| ext4-point8 | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-ext4-point8.jsonl) | [TSV](artifacts/rfc-025-v7-batch-membership-20261027-ext4-point8-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-ext4-point8-host.jsonl) |
| confirmation-batch8 | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-confirmation-batch8.jsonl) | [TSV](artifacts/rfc-025-v7-batch-membership-20261027-confirmation-batch8-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-membership-20261027-confirmation-batch8-host.jsonl) |


Saved executables, perf data, stdout/stderr and validation logs stay under ignored `target/wal-opt` as local hash-check evidence. Disposable databases and reconstruction scratch are removed. [WAL documentation](../README.md).
