# RFC 025: compact batch geometry and an added-wait cost limit, 2026-10-27

**Rejected.** Batch16 passes its v7 guards, but point8 loses 13.9% paired throughput and raises p99 17.4%. The three dependent batch stages are cancelled. The source is restored to kept f7 before a separately planned final metadata-retirement correction. CPU does not veto retention.

The [first minimum-geometry prototype](rfc-025-v7-batch-membership-20261027.md) recognizes packed logical batches correctly but fails throughput/p99 at sixteen writers, point throughput and independent batch8 confirmation. This corrective source keeps accurate membership while addressing two avoidable costs exposed by validation. The first cohort remains rejected and is never pooled into the new assessment. Both prototypes compare against the kept `d7fbe1c9` source and `f7fd96e5…` executable, starting from `dc3e3c1a`.

## Corrections

The original completion result occupies 64 bytes on the tested 64-bit target. Adding a `u64` minimum expands it to 72. The corrective result uses a checked `u32` per-group write count and a `u32` minimum. A DATA write is already validated below `i32::MAX`; group length is checked before conversion. Lifetime request IDs, logical tickets, byte totals and profile counters remain 64-bit, with explicit widening at the boundary. The actual worker test asserts the completion remains **64 bytes**. Group/command ownership metadata still carries the geometry; this is not a claim of eliminating all per-group storage.

The first prototype also makes more packed waves eligible for an 800-microsecond wait after a sync costing only 100 microseconds. In its first fast 16-writer pair, coalescing time rises from 572.5 to 740.1 ms while throughput falls from 527,454 to 442,851 puts/s, despite fewer syncs. The correction permits the extra window only when the previous sync covers its additional cost:

```text
ordinary slow-sync bound: 400 microseconds
large-wave bound:         800 microseconds
added wait budget:       400 microseconds

use 800us only if previous fdatasync >= 400us
```

Otherwise a slow-sync wave keeps 400 microseconds; fast v7 cycles keep 100; first-cycle/v4 behavior retains its previous selection. Catching admission still returns immediately. The rule estimates whether avoiding a sync can pay for the extra wait; it does not guarantee optimal batching or predict every device stall.

The worker computes minimum DATA size while it already validates a group, excluding reserved FRONTIER gaps. The coordinator requires one DATA write per logical ticket, a nonzero count and a minimum of 64 KiB. Mixed and unavailable geometry remain conservative. There is no added admission lookup, lock, hash, allocation policy, thread/runtime switch, allocator or memtable policy. Persisted layout, digests, ticket order, recovery and one-sync-before-ACK behavior do not change.

## Validation

The final source passes **1,973 tests**, zero skipped/retried, outside the sandbox, plus eight focused tests with immediate output. Existing tests validate real v7 all-large/mixed packed groups, exact recovery, buffer release, out-of-order/short CQEs, FRONTIER gaps, unavailable geometry, poison and recapture. The policy test covers the 100/399/400/800-microsecond boundaries and preserves point/v4/fast-sync selection. The worker test checks actual 64-byte result size. The initial compile catches a missing explicit widening of one compact count; its failure log is preserved, and no scored command uses that version. Full formatting, Clippy, dependency, spelling and release checks pass after the correction.

The three preceding baseline user-CPU profiles are reused, counted once, and excluded from scoring. Their metadata and default-leveled versus scored-no-compaction distinction are described in the first report. No new profile or source modification overlaps scored commands.

## Frozen stages

Batch16 runs first, with one warmup per arm and three alternating four-arm rounds. Passing its v7 throughput/p99/DATA guards permits point8, batch1, batch8 and independent batch8 confirmation in that order. Each completed stage is assessed before advancing. A failed stage explicitly cancels unrun dependent cells; no slow score is retried or replaced.

Configuration matches the first prototype: fresh ext4 SSD databases, 1-KiB values, 2-GiB SST target, explicit Parallel v4/v7, 750,000 puts for batch64 with sampling every ten batches, and 100,000 point puts with sampling every 100. Initial cooldown is 30 seconds; gaps are 10 seconds. The process audit samples recognized competing work before/during 1 Hz/after each command. CPU covers timed process user+system time; exact-child wait4 peak RSS covers setup/close too. Storage variation and post-measurement work limit causal interpretation; sampling cannot exclude every unrecognized or brief process.

Each v7 stage requires paired and separate-arm median throughput at least 0.95x, p99 at most 1.10x and identical DATA byte counts. Independent batch8 confirmation must improve both throughput (>1.00x) and p99 (<1.00x). **CPU is diagnostic without a retention veto.** V4 controls, sync counts, RSS and normalized ratios remain visible. Complete qualification of this slice requires every planned stage; it is not the full RFC default-adoption gate.

## Results

### V7

| Workload | Retained puts/s | Candidate puts/s | Paired throughput | p99 | CPU | Syncs | Guards |
|---|---:|---:|---:|---:|---:|---:|---|
| ext4-batch16 | 101,088 | 380,655 | +225.7% | -69.8% | -36.3% | -1.2% | Pass |
| ext4-point8 | 15,959 | 5,418 | -13.9% | +17.4% | +3.1% | +0.2% | Fail |

### V4 controls

| Workload | Retained puts/s | Candidate puts/s | Paired throughput | p99 | CPU | Syncs | Guards |
|---|---:|---:|---:|---:|---:|---:|---|
| ext4-batch16 | 556,568 | 113,887 | -77.5% | +837.8% | +27.7% | -2.3% | Fail |
| ext4-point8 | 17,263 | 17,268 | +0.6% | -6.5% | +0.9% | +0.0% | Pass |

The batch16 paired gain is strongly affected by slow baseline runs; its v4 control fails with throughput 0.225x and p99 9.38x. Point8 v4 stays near parity while v7 fails. Candidate point8 has a fast run close to baseline, but all slow records remain in the assessment. These results reject this exact candidate without proving every point slowdown comes from the source. Batch1, batch8 and independent confirmation are explicitly unrun.

## Highest matching scores

| Workload | Retained v4 | Candidate v4 | Retained v7 | Candidate v7 |
|---|---:|---:|---:|---:|
| ext4-batch16 | 584,796 | 131,379 | 164,182 | 534,697 |
| ext4-point8 | 17,514 | 17,375 | 16,036 | 15,961 |

The corrective batch16 peak is 534,697 puts/s. The strongest retained f7 batch16 score in the preceding identical-config cohort is 534,373 puts/s, so these observed peaks differ by only 0.06%. This cross-cohort comparison is descriptive; it does not turn the failed point stage into a pass.

Highest results are descriptive. Matched historical scores from the first source cohort use the identical retained executable/configuration and remain separate from the new paired estimates. Failed controls and storage stalls are never filtered.

## Final state

All 32 invocations remain: 24 scored and eight warmups. Three diagnostic profiles are reused and counted once. The saved `0e2c8315…` executable and exact rejected patch remain. The restoration records actual f7 source and canonical executable state before the [final retirement-hint variant](rfc-025-v7-batch-retirement-hint-20261027.md). The latter owns final live-state verification; this source is not committed as an optimization.

The verifier reconstructs all 128 build inputs from the base and exact patch, checks saved executables, regenerates summaries, validates raw/command/audit identities and sample totals, and checks final source/executable state. The first prototype's historical restoration is bound to the actual restored f7 image before this corrective edit. Local binaries/logs/perf files remain ignored evidence; disposable databases and reconstruction scratch are removed.

## Evidence

- [Frozen corrective plan](artifacts/rfc-025-v7-batch-wait-budget-20261027-plan.json)
- [Stage matrix](artifacts/rfc-025-v7-batch-wait-budget-20261027-matrix.json)
- [Exact final candidate patch](artifacts/rfc-025-v7-batch-wait-budget-20261027-candidate.patch)
- [128-input manifest](artifacts/rfc-025-v7-batch-wait-budget-20261027-source-sha256.tsv)
- [Saved executable identities](artifacts/rfc-025-v7-batch-wait-budget-20261027-binaries.json)
- [Environment](artifacts/rfc-025-v7-batch-wait-budget-20261027-environment.json)
- [Checks and failed compile](artifacts/rfc-025-v7-batch-wait-budget-20261027-checks.json)
- [Stage controller](artifacts/rfc-025-v7-batch-wait-budget-20261027-run-all.py)
- [Summary](artifacts/rfc-025-v7-batch-wait-budget-20261027-summary.json)
- [Highest scores](artifacts/rfc-025-v7-batch-wait-budget-20261027-peaks.json)
- [Decision](artifacts/rfc-025-v7-batch-wait-budget-20261027-decision.json)
- [Verifier](artifacts/rfc-025-v7-batch-wait-budget-20261027-verify.py)
- [Verification result](artifacts/rfc-025-v7-batch-wait-budget-20261027-verification.json)

| Cell | Raw records | Commands/identities | Process audit |
|---|---|---|---|
| ext4-batch16 | [JSONL](artifacts/rfc-025-v7-batch-wait-budget-20261027-ext4-batch16.jsonl) | [TSV](artifacts/rfc-025-v7-batch-wait-budget-20261027-ext4-batch16-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-wait-budget-20261027-ext4-batch16-host.jsonl) |
| ext4-point8 | [JSONL](artifacts/rfc-025-v7-batch-wait-budget-20261027-ext4-point8.jsonl) | [TSV](artifacts/rfc-025-v7-batch-wait-budget-20261027-ext4-point8-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-wait-budget-20261027-ext4-point8-host.jsonl) |

[WAL documentation](../README.md).
