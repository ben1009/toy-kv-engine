# RFC 025: derive batch geometry at worker retirement, 2026-10-27

**Rejected; existing batch optimization retained.** Point8 passes, but batch16 loses 1.0% paired throughput and raises p99 23.1%. The p99 check stops dependent stages. The kept `d7fbe1c9` source and `f7fd96e5…` executable are restored. The three-variant investigation adds no retained source change.

This is the final bounded correction to the [batch-membership investigation](rfc-025-v7-batch-membership-20261027.md). The [compact-result and wait-budget variant](rfc-025-v7-batch-wait-budget-20261027.md) restores fast 16-writer throughput but fails point8. Its completion stays 64 bytes, yet group state still grows from 96 to 104 bytes. This variant eliminates that extra group metadata; it does not assume the layout alone explains every failed SSD score.

The worker already removes each `OwnedWrite` from its map after terminal completion. Its validated `write_len` survives buffer retirement. Compute the minimum DATA-write length in that existing removal loop, excluding FRONTIER gaps naturally. `WriteGroup` and `GroupState` definitions retain their original layout. A checked `u32` count and minimum fit within the original 64-byte completion; group state remains 96 bytes on the tested 64-bit target. Failure snapshots with unavailable geometry report zero conservatively. Lifetime identities, tickets, byte totals and profile counters remain 64-bit.

The coordinator recognizes a wave only if each ticket has one DATA write and every write is at least 64 KiB. The prior cost limit remains: an 800-microsecond large-wave bound requires the previous sync to cost at least its 400-microsecond additional wait. Other slow waves keep 400 microseconds and fast v7 cycles keep 100; catching admission exits immediately. No persisted bytes, hash, recovery, ticket order, one-sync-before-ACK, allocator, memtable, extent worker or runtime switch.

## Validation and fixed comparison

The candidate passes **1,973 tests**, zero skipped/retried, and eight focused tests outside the sandbox. Formatting, Clippy, dependency checks, direct spelling and the release build pass. Focused tests check actual 64-byte completion/96-byte group sizes, min-size metadata across FRONTIER gaps and reversed/short CQEs, conservative failure snapshots, real large/mixed packed v7 groups, exact recovery, recapture, poison and buffer release. The policy test exercises the 100/399/400/800-microsecond boundaries. No failed build is omitted from this final variant.

Three fresh baseline profiles are reused from the first variant, excluded and counted once. They show 63.3% and 73.6% of logical batches in packed groups at eight and sixteen writers. Profiles use default leveled compaction and whole-child user-CPU samples; scoring explicitly uses no compaction. They establish membership coverage rather than a paired performance estimate or timed-path CPU attribution.

Point8 runs first. Only a passing v7 stage permits batch16, batch1, batch8 and an independent batch8 confirmation in that order. Each stage has four warmups and three alternating four-arm rounds: retained/candidate v4/v7, with every slow score retained. Configuration is ext4 SSD, 1-KiB values, 2-GiB SST target, explicit Parallel, 100,000 point puts sampled every 100 or 750,000 puts in batches of 64 sampled every ten batches. Initial cooldown is 30 seconds and gaps are 10 seconds. No build, test, profile or source modification overlaps scoring.

Each stage requires paired and separate-arm median v7 throughput >=0.95x, p99 <=1.10x and equal DATA byte counts. Independent confirmation additionally needs throughput >1.00x and p99 <1.00x. CPU/RSS/sync counts and v4 controls stay diagnostic; CPU has no veto. A failed stage cancels its dependents, with no replacement run or guard override. Process audits cover recognized competing work before/during 1 Hz/after; they cannot rule out every brief/unrecognized process or storage stall. Whole-child RSS includes setup/close; timed CPU is user+system CPU during writes.

## Results

### V7

| Workload | Retained puts/s | Candidate puts/s | Paired throughput | p99 | CPU | Syncs | Guards |
|---|---:|---:|---:|---:|---:|---:|---|
| ext4-point8 | 5,123 | 12,146 | +133.7% | -7.4% | -12.4% | -3.6% | Pass |
| ext4-batch16 | 288,686 | 406,519 | -1.0% | +23.1% | -10.7% | +1.0% | Fail |

### V4 controls

| Workload | Retained puts/s | Candidate puts/s | Paired throughput | p99 | CPU | Syncs | Guards |
|---|---:|---:|---:|---:|---:|---:|---|
| ext4-point8 | 5,585 | 5,575 | +0.5% | -11.0% | -1.5% | -0.3% | Pass |
| ext4-batch16 | 115,762 | 117,586 | +4.5% | -9.4% | -7.2% | -3.3% | Pass |

Point8 throughput is inflated by slow retained runs; this does not establish a 2.34x source gain. Batch16 v4 controls pass, but v7 p99 fails. Both v7 arms still have variable storage stalls. Lower candidate CPU does not rescue the failed p99 result, just as higher CPU would not veto a throughput/p99 gain. Batch1, batch8 and confirmation are cancelled without invoking them. No performance parity or RFC default-adoption qualification is claimed.

## Highest matching scores

| Workload | Retained v4 | Candidate v4 | Retained v7 | Candidate v7 |
|---|---:|---:|---:|---:|
| ext4-point8 | 13,172 | 15,031 | 5,198 | 14,292 |
| ext4-batch16 | 117,022 | 561,250 | 508,250 | 503,229 |

Highest scores are descriptive and never replace the frozen paired assessment. Failed controls and all raw records remain visible.

## Final state

All 32 final-variant invocations remain: 24 scored and eight warmups. Across the three prototypes, the investigation preserves **144 scored/warmup invocations** (108 scored, 36 warmups) plus **three excluded fresh profiles**, counted once. The failed variants use distinct source identities and are not pooled into a new estimate. The final unique-invocation audit includes 384 records across this investigation and its preceding cohorts.

The two Rust files exactly match the retained base `dc3e3c1a`; canonical `target/release/write-perf` again matches retained f7. Restored source passes all **1,971 tests** outside the sandbox. The first restoration check reused a cached 1,973-test candidate binary because copied files retained older timestamps; that log is preserved and excluded from restored validation (it also contains one unrelated compaction retry). Touching the two restored files forces recompilation without changing their bytes; final verification requires the rebuilt 1,971-test log. The rejected `54b0c2dc…` candidate, its exact patch and 128-input manifest remain as evidence. There is no new source commit, push, PR or format change. Saved binaries, logs and profiles remain ignored local evidence; disposable databases and reconstruction scratch are removed. The next step is review of this bounded slice.

The [restoration record](artifacts/rfc-025-v7-batch-retirement-hint-20261027-restoration.json) binds actual source/executable hashes. The verifier reconstructs each candidate from the original base plus patch, validates saved binaries, regenerates score summaries, checks command/raw/audit identities and sample counts, validates the two earlier historical restorations, and verifies final live source/executable state.

## Evidence

- [Frozen final plan](artifacts/rfc-025-v7-batch-retirement-hint-20261027-plan.json)
- [Ordered stage matrix](artifacts/rfc-025-v7-batch-retirement-hint-20261027-matrix.json)
- [Exact source patch](artifacts/rfc-025-v7-batch-retirement-hint-20261027-candidate.patch)
- [128-input source manifest](artifacts/rfc-025-v7-batch-retirement-hint-20261027-source-sha256.tsv)
- [Saved executables](artifacts/rfc-025-v7-batch-retirement-hint-20261027-binaries.json)
- [Environment](artifacts/rfc-025-v7-batch-retirement-hint-20261027-environment.json)
- [Checks](artifacts/rfc-025-v7-batch-retirement-hint-20261027-checks.json)
- [Stage controller](artifacts/rfc-025-v7-batch-retirement-hint-20261027-run-all.py)
- [Summary](artifacts/rfc-025-v7-batch-retirement-hint-20261027-summary.json)
- [Highest scores](artifacts/rfc-025-v7-batch-retirement-hint-20261027-peaks.json)
- [Decision](artifacts/rfc-025-v7-batch-retirement-hint-20261027-decision.json)
- [Verifier](artifacts/rfc-025-v7-batch-retirement-hint-20261027-verify.py)
- [Verification result](artifacts/rfc-025-v7-batch-retirement-hint-20261027-verification.json)

| Cell | Raw records | Commands/identities | Process audit |
|---|---|---|---|
| ext4-point8 | [JSONL](artifacts/rfc-025-v7-batch-retirement-hint-20261027-ext4-point8.jsonl) | [TSV](artifacts/rfc-025-v7-batch-retirement-hint-20261027-ext4-point8-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-retirement-hint-20261027-ext4-point8-host.jsonl) |
| ext4-batch16 | [JSONL](artifacts/rfc-025-v7-batch-retirement-hint-20261027-ext4-batch16.jsonl) | [TSV](artifacts/rfc-025-v7-batch-retirement-hint-20261027-ext4-batch16-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-retirement-hint-20261027-ext4-batch16-host.jsonl) |

[WAL documentation](../README.md).
