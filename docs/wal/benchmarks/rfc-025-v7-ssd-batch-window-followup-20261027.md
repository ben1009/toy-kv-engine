# RFC 025: remaining SSD batch-window qualification, 2026-10-27

**Result: keep the exact batch-window source provisionally for local review under the maintainer’s throughput/p99 preference. The six-cell performance assessment remains failed at single-writer batch, and the additional same-binary control is inconclusive. This retention is an explicit exception to the automatic all-cell gate, rather than a claim of full qualification.**

The maintainer requests the remaining qualification after reviewing the initial batch-only screen and states that throughput and p99 improvements justify keeping a change despite a small CPU increase. This follow-up restores the exact previous candidate; it does not tune a new policy or relabel the original zero-CPU-gate failure.

The initial screen observed paired batch8 throughput +3.3%, p99 -23.5%, sync count -17.1% and CPU +0.5%. The old rule stopped there. Its [report](rfc-025-v7-ssd-batch-and-extent-20261027.md), raw data, decisions and restored-state verification remain historical evidence. The continuation's criteria are fixed before any new measurement and recorded in a separate plan.

## Candidate and validation

After a sync lasting at least 100 microseconds, known large v7 waves may coalesce for up to 800 microseconds. Eligibility conservatively requires every group in the initial contiguous unsynced wave to have exactly one ticket and at least 64 KiB of DATA. Small, mixed or unknown multi-ticket waves keep the prior policy. Fast v7 syncs keep 100 microseconds; point/v4 timing selection stays as before. Catching admission exits immediately, so one writer does not pay a compulsory grace. A fixed deadline bounds the wait.

The candidate Boolean advances only with the contiguous written prefix, resets after a durable cutoff, and never advances durability. FRONTIER capture still precedes one fdatasync and ACK. File layout, persisted hashes, recovery, ownership, reservations, extent preparation, System allocator and SkipMap remain the same.

All 128 Rust/Cargo/toolchain build inputs match the previous candidate. Its existing outside-sandbox full **1,971-test** check and **12 focused tests** are reused with exact source/log hashes; the already saved executable is used again without a rebuild. The tests cover real encoded DATA with out-of-order CQEs, small/mixed/unknown waves, durable cutoffs, resource release, close, recovery and the existing paused-worker/poison cases. No further Rust source edits occur during qualification.

- Retained baseline executable: `e7f7712825d2166c14b5b67601388a8e29697f8fc034832e457061cf6e30ae19`.
- Batch-window executable: `f7fd96e5ca0ecaaaa23f59f135e7ce9430521f986834e5c95f2d32070e378d54`.
- Continuation source base: `7aefc27a`, which has the same engine source as the initial base `53f48459`.

## Protocol and decision rule

The six remaining cells are point8, batch1, batch16, point1, point16 and independent batch8 confirmation. All six run even when a performance guard fails. Each has four warmups and three alternating four-arm rounds: baseline v4, candidate v4, baseline v7, candidate v7; even rounds reverse the order. This adds 72 scored invocations and 24 warmups. The initial five-round batch8 screen remains a separate cohort and is not pooled into the confirmation estimate.

All use fresh databases on ext4 `/dev/nvme0n1p3`, 1-KiB values, explicit Parallel selectors and a 2-GiB SST target. Point1 uses 20,000 puts with latency sampling every ten operations. Point8/16 use 100,000 puts and sample every 100. Batch64 uses 750,000 puts and samples every ten batches. Exact operation and sample totals are recomputed from the per-writer remainder distribution. A 30-second initial cooldown and ten-second intercommand gaps are fixed. No build, test or profile overlaps scoring.

Provisional retention requires all cells complete, identical DATA byte counts, v7 paired and separate-arm median throughput at least 0.95x, and paired p99 at most 1.10x in every cell. Independent batch8 confirmation must show paired throughput above 1.00x and p99 below 1.00x. **CPU is reported, without a retention veto.** RSS, sync counts and v4 controls are also reported diagnostics. This follows the maintainer's throughput/p99 preference and does not retrospectively change the earlier experiment's rule.

Every completed record, storage stall and failed control stays in the analysis. No slow score is filtered or selectively retried. Recognized competing processes are sampled before, once per second during, and after commands; sampling cannot exclude all short-lived or unrelated work. Moving SSD stalls, v4 post-measurement drain/SST work and different PITR close behavior can affect subsequent device state despite the cooldown. Failed controls limit causal interpretation; this slice does not qualify the complete RFC default-adoption gate.

## Results

### V7

| Workload / writers | Baseline puts/s | Candidate puts/s | Paired throughput | p99 | CPU | Syncs | Performance guards |
|---|---:|---:|---:|---:|---:|---:|---|
| Point / 8 | 5,242 | 5,348 | -3.0% | +1.8% | -2.5% | +0.6% | Pass |
| Batch64 / 1 | 41,656 | 28,820 | -43.8% | +10.3% | +9.5% | +0.0% | Fail |
| Batch64 / 16 | 97,302 | 101,199 | +2.2% | -24.9% | +0.3% | -6.7% | Pass |
| Point / 1 | 1,815 | 2,130 | +17.3% | -51.1% | -1.3% | +0.0% | Pass |
| Point / 16 | 13,019 | 12,824 | +24.6% | -25.0% | -9.4% | -1.4% | Pass |
| Batch64 / 8 confirmation | 151,803 | 387,107 | +120.3% | -78.2% | -18.6% | -22.7% | Pass |

### V4 controls

| Workload / writers | Baseline puts/s | Candidate puts/s | Paired throughput | p99 | CPU | Syncs | Performance guards |
|---|---:|---:|---:|---:|---:|---:|---|
| Point / 8 | 10,399 | 5,859 | -47.5% | +7.7% | +13.4% | +0.1% | Fail |
| Batch64 / 1 | 23,891 | 30,078 | +28.8% | -1.1% | -9.4% | +0.0% | Pass |
| Batch64 / 16 | 395,543 | 465,074 | -6.5% | +6.1% | +2.5% | +0.7% | Fail |
| Point / 1 | 1,744 | 2,144 | -1.7% | -0.4% | +2.6% | +0.0% | Pass |
| Point / 16 | 9,252 | 9,163 | -1.8% | -8.3% | +5.6% | +0.9% | Pass |
| Batch64 / 8 confirmation | 135,553 | 73,202 | +1.9% | +12.1% | +1.0% | +2.5% | Fail |

All six cells ran, including the independent confirmation after the single-writer failure. Five v7 cells pass throughput/p99 guards. Single-writer batch fails paired and arm-median throughput and paired p99; CPU is not a veto. The complete six-cell assessment therefore remains failed even though independent batch8 confirmation and batch16 both show throughput/p99 improvements. V4 point8, batch16 and batch8 confirmation controls fail, limiting attribution of measured changes.

Batch8 confirmation reduces median sync count by 22.7% paired; batch16 reduces it by 6.7%. One-writer batches use exactly 11,719 syncs per run, with **zero coalescing wait calls and nanoseconds in every scored arm**. The candidate does not pay a compulsory 800-microsecond sleep. The original batch1 baseline fdatasync totals span 3.66–11.46 seconds, versus 8.24–17.07 seconds for the candidate. These timers expose large I/O variation but do not alone prove whether every throughput difference is environmental.

The extra [single-writer control plan](artifacts/rfc-025-v7-ssd-batch-solo-control-20261027-plan.json) freezes one independent cohort with reference and repeat arms using the identical baseline executable, plus the unchanged candidate. It preserves the original failure and cannot turn the six-cell assessment into a pass. It tests confidence in a source-level solo regression before the final retention judgment.

| Additional solo comparison | Paired throughput | p99 | CPU | Regression guards |
|---|---:|---:|---:|---|
| Identical baseline repeat / reference | +179.4% | -85.4% | -11.7% | Pass |
| Candidate / baseline reference | +8.6% | +2.2% | +0.0% | Pass |
| Candidate / baseline repeat | -59.4% | +728.4% | +9.4% | Fail |

The identical-baseline repeat/reference estimate is **2.794x throughput and 0.146x p99**. Its one-sided regression guards pass because the second baseline is faster, but this is **not a stable null control**. Across the five pairs, identical-baseline throughput ratios range from 1.224x to 2.878x. The candidate/reference comparison passes (throughput +8.6%, p99 +2.2%), while candidate/repeat fails (throughput -59.4%, p99 8.284x). Every scored solo invocation still has zero coalescing wait calls/time and identical DATA/sync counts.

This demonstrates substantial run/order variation even without a source change. It neither proves that the candidate has no solo regression nor isolates one. All 18 extra invocations remain: 15 scored and three warmups. They do not replace the original failed solo records or change the six-cell assessment. The candidate is retained provisionally because the maintainer prioritizes the repeated multithreaded throughput/p99 benefit; single-writer qualification remains unresolved. No additional source policy or score-selected rerun follows this cohort.

- [Solo control summary](artifacts/rfc-025-v7-ssd-batch-solo-control-20261027-summary.json)
- [Solo stability interpretation](artifacts/rfc-025-v7-ssd-batch-solo-control-20261027-interpretation.json)
- [Solo raw data](artifacts/rfc-025-v7-ssd-batch-solo-control-20261027-ext4-batch1.jsonl)
- [Solo command/identity manifest](artifacts/rfc-025-v7-ssd-batch-solo-control-20261027-ext4-batch1-runs.tsv)
- [Solo host audit](artifacts/rfc-025-v7-ssd-batch-solo-control-20261027-ext4-batch1-host.jsonl)



CPU is timed process user plus system time for the measured writes. Linux wait4 peak RSS covers the exact child process's whole lifetime, including setup and close. Throughput and p99 changes are medians of within-round candidate/baseline ratios; absolute medians can have a different quotient under moving stalls. Point p99 covers a put; batch p99 covers one whole 64-put batch.

## Highest scores

| Workload / writers | Highest baseline puts/s | Highest candidate puts/s | Baseline run | Candidate run |
|---|---:|---:|---|---|
| Point / 8 | 16,557 | 13,942 | `1793071368234` | `1793071384409` |
| Batch64 / 1 | 70,640 | 41,405 | `1793071966907` | `1793072176724` |
| Batch64 / 16 | 98,997 | 469,749 | `1793072336608` | `1793072501194` |
| Point / 1 | 2,365 | 2,175 | `1793072706356` | `1793072724930` |
| Point / 16 | 24,571 | 25,332 | `1793073246155` | `1793073129004` |
| Batch64 / 8 confirmation | 180,477 | 397,622 | `1793073475476` | `1793073463358` |

Highest scores are descriptive. They do not replace fixed paired comparisons or remove a slow run. The peak record binds every winner to its run ID, exact raw line and executable hash. V4 controls and v7/v4 normalized ratios are also retained, without treating a failed control as proof of an algorithmic speedup or regression.

## Final state and verification

The candidate remains applied, with the same runtime and saved executable hashes as the original batch screen. No source retuning was needed for this follow-up. The source change and its tests are committed separately as `d7fbe1c9`. The exact prior 1,971-test full check and 12 focused tests are reused under the 128-input equality proof; this report does not claim a new full test run. Formatting/spelling and complete artifact verification run after timing ends.

The automatic six-cell assessment remains **failed**, with CPU diagnostic only. Provisional retention is a documented maintainer-preference exception: repeated multithreaded throughput/p99 improvements support keeping the candidate for review, while same-binary solo variation prevents a strong source-level conclusion. No default activation, v4 parity, complete Stage 8 qualification, push or PR is implied.

The follow-up preserves 96 invocations (72 scored / 24 warmups), plus 18 additional solo-control invocations (15 scored / three warmups). Together with the earlier three diagnostic and 120 measured/warmup invocations, all **237 invocation IDs** are distinct. The continuation and solo control contribute 1,970 clear recognized-process audit samples; sampling does not exclude every unrecognized or brief task. The baseline input manifest is independently checked against its original saved-executable manifest; only `runtime.rs` differs in the candidate. Both the initial failure and the new retention exception remain reviewable.

The verifier reconstructs the candidate from the continuation base and exact patch, checks every build-input hash and both saved executables, regenerates the complete summary, verifies sample counts and run order, and checks command/raw/audit hashes. It validates the original trial as a frozen historical bundle at its own evidence commit, rather than requiring the earlier baseline-only live-source assertion after a legitimate new trial. Original trial artifacts are unchanged. A post-timing spelling correction affects only the quoted user preference in the continuation plan; the original frozen bytes are preserved in a compressed plan, and an erratum binds both hashes. Criteria and schedules are unchanged. Cross-cohort run-ID checks count reused diagnostics once. Temporary benchmark databases and reconstruction scratch are removed.

## Evidence

- [Frozen continuation plan](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-plan.json)
- [Fixed six-cell matrix](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-matrix.json)
- [Exact candidate patch](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-candidate.patch)
- [128-input manifest](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-source-sha256.tsv)
- [Saved executable identities](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-binaries.json)
- [Driver for all cells](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-run-all.py)
- [Per-cell driver](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-run.py)
- [Environment](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-environment.json)
- [Checks](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-checks.json)
- [Source identity proof](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-source-validation.json)
- [Mechanism counters](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-mechanism.json)
- [Plan spelling erratum](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-plan-erratum.json)
- [Current candidate state](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-live-state.json)
- [Summary](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-summary.json)
- [Highest scores](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-peaks.json)
- [Decision](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-decision.json)
- [Verifier](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-verify.py)
- [Verification result](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-verification.json)

| Cell | Raw data | Commands and identities | Host audit |
|---|---|---|---|
| ext4-point8 | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point8.jsonl) | [TSV](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point8-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point8-host.jsonl) |
| ext4-batch1 | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-batch1.jsonl) | [TSV](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-batch1-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-batch1-host.jsonl) |
| ext4-batch16 | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-batch16.jsonl) | [TSV](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-batch16-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-batch16-host.jsonl) |
| ext4-point1 | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point1.jsonl) | [TSV](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point1-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point1-host.jsonl) |
| ext4-point16 | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point16.jsonl) | [TSV](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point16-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-ext4-point16-host.jsonl) |
| confirmation-batch8 | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-confirmation-batch8.jsonl) | [TSV](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-confirmation-batch8-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ssd-batch-window-followup-20261027-confirmation-batch8-host.jsonl) |


Saved binaries and validation logs under `target/wal-opt` are local, ignored evidence needed for local hash checks. They are not repository payloads. [WAL documentation](../README.md).
