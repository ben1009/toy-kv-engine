# RFC 025: allocator retry after SSD scheduling improvements, 2026-10-27

**Result: the allocator retry is not adopted. The full mimalloc policy fails the fixed SSD assessment and increases peak RSS. System and the canonical retained executable are restored; the kept async extent and single-writer SSD improvements remain unchanged.**

The maintainer requests another allocator test after keeping asynchronous v7 extent preparation and the serial scheduling fix. This experiment starts from `e16e433a90a826d4f51fd8e29e5ec14f8f1ae0fc`, whose last source change is the hybrid preparation policy in `71bd7c23`. The WAL runtime source is verified unchanged throughout the allocator trial. There is no Tokio migration or new I/O policy.

## Allocator scope

The candidate repeats the [earlier full allocator policy](rfc-025-v7-mimalloc-20261026.md): process-wide Rust `mimalloc::MiMalloc`, plus aligned WAL buffers allocated and freed through matching `std::alloc::Layout(capacity, 4096)`. Default feature selection retains Rust System and libc `posix_memalign/free`. The optional feature is `mimalloc-allocator`. C malloc overrides, secure mode and runtime allocator tuning remain disabled. Address/Leak sanitizer configurations select System. Capacity, page alignment, buffer pooling, reservation pins, I/O ownership and on-disk bytes remain unchanged.

This is the same published Rust wrapper/native combination as the earlier test: **mimalloc 0.1.52**, **libmimalloc-sys 0.1.49**, bundled native **v3.3.2**. The current [published Rust wrapper](https://docs.rs/crate/mimalloc/0.1.52) still declares that sys-crate dependency. Newer [upstream native releases](https://github.com/microsoft/mimalloc) are outside this controlled repeat. The activation proof checks the native version in an excluded `--help` process and confirms that the candidate does not define C malloc/free override symbols. Allocator verbosity is never enabled during scoring.

An excluded current-baseline SSD profile precedes the source change. It records 596 `cpu-clock:u` samples with zero lost samples. Allocator calls do not dominate the flat user-CPU profile; it does not measure kernel CPU or off-CPU I/O waits and does not predict the allocator's cache/locality effects. No profile record enters performance scoring.

## Fixed SSD protocol

Six cells cover one, eight and sixteen writers for point puts and batch64. Every cell has five excluded warmups and exactly three alternating five-arm rounds: unchanged retained v7, paired-source System v4, mimalloc v4, paired-source System v7 and mimalloc v7. All use ext4 on the local SSD, 1-KiB values, a 2-GiB SST target and explicit Parallel format selectors. A 30-second initial cooldown and ten seconds between commands are fixed before scoring.

One-writer point runs use 20,000 puts and sample every ten operations (2,000 samples). Eight/sixteen-writer point runs use 100,000 puts and sample every 100 (1,000 / 1,008 samples). Batch64 uses 750,000 puts and samples every ten batches (1,172 / 1,176 / 1,184 samples). Remainder distribution across writers explains the distinct sample totals; every capture is validated against its exact configuration.

The System and mimalloc executables are compiled from the same prototype source with the allocator feature disabled/enabled. The paired System executable is not byte-identical to the unchanged retained SSD binary, so a separate unchanged-v7 arm runs in every round. Its same-format comparisons remain visible. Both formats change allocator; v7/v4 normalization is diagnostic rather than an allocator acceptance guard.

Each format/cell requires paired raw and arm-median throughput at least 0.95x, CPU at most 1.05x, p99 at most 1.10x, equal DATA bytes and sync-count ratio at most 1.05x. At least one format/cell must gain 2% raw with CPU at most unchanged. A peak-RSS ratio above 1.20x is a memory-review flag. These are exploratory assessment rules; this test does not automatically change the default allocator or qualify the complete RFC performance gate.

Recognized competing builds, tests and benchmarks are audited before, once per second during, and after each command. There is no build/test/profile overlap with scoring. Sampling cannot exclude every brief or unrelated workload. All stalls, including those in the unchanged retained arm, remain included. There are no RAM performance captures or score-selected retries.

## Measurement scopes and checks

CPU and throughput cover the engine's measured write interval. RSS, faults and context switches come from Linux `wait4` for the exact fork/exec engine child. They cover the whole child lifetime, including setup and close. They do not use the launcher process's high-water mark. V4 performs its existing post-measurement drain/SST work, so cross-format RSS values have different work scope; allocator ratios are compared within the same format. Peak RSS is not a claim about long-running steady-state memory retention.

Both configurations pass all **1,970 normal tests** outside the sandbox. Mimalloc's full `cargo make check` passes formatting, Clippy, dependency checks, tests and spelling. System uses all existing feature flags except the allocator and runs the normal library/binary/integration test targets with zero retries. An additional focused mimalloc test executes alignment, contents and cross-thread destruction across multiple capacities. Both release builds and direct spelling checks pass.

The first System validation command mistakenly selected `--all-targets`, which included Criterion benchmark executables under the normal test timeout. It records 2,054 passes and 27 benchmark timeouts. The corrected `--lib --bins --tests` command passes all 1,970 normal tests without a source change. The original timeout log remains in the checks record; those Criterion executions are excluded from SSD scoring.

The exact saved executables are:

- Unchanged retained SSD v7: `e7f7712825d2166c14b5b67601388a8e29697f8fc034832e457061cf6e30ae19`.
- Paired-source System: `37a824c5f64874f3633fa0efca25c1c260dd73f837dc4ab18e56f767040fb8cc`.
- Mimalloc with aligned WAL buffers: `1affcad90fd93ed9007d92d779706c48b89bcd36e000b33051c8a66e06983c01`.
## V7 allocator comparison

Changes are medians of the three within-round mimalloc/System ratios. Throughput columns are separate arm medians; their quotient can differ from the paired change. CPU and p99 are lower-is-better. Point p99 covers a put; batch p99 covers a complete 64-put batch. RSS is the median ratio of engine-child peak RSS.

| Workload | System puts/s | Mimalloc puts/s | Paired throughput | CPU | p99 | RSS ratio | Guards |
|---|---:|---:|---:|---:|---:|---:|---|
| Point / 1 writer(s) | 935 | 1,116 | +0.4% | -1.3% | -2.4% | 2.17x | Pass |
| Batch64 / 1 writer(s) | 66,111 | 63,755 | +10.3% | -15.2% | -1.1% | 1.22x | Pass |
| Point / 8 writer(s) | 15,795 | 9,094 | -1.0% | -0.5% | -4.2% | 1.44x | Fail |
| Batch64 / 8 writer(s) | 405,202 | 342,403 | -15.5% | +56.8% | +9.9% | 1.23x | Fail |
| Point / 16 writer(s) | 13,681 | 6,646 | -43.0% | +29.7% | +8.9% | 1.47x | Fail |
| Batch64 / 16 writer(s) | 537,235 | 520,578 | -3.0% | +27.3% | -10.0% | 1.23x | Fail |

## V4 allocator comparison

Changes are medians of the three within-round mimalloc/System ratios. Throughput columns are separate arm medians; their quotient can differ from the paired change. CPU and p99 are lower-is-better. Point p99 covers a put; batch p99 covers a complete 64-put batch. RSS is the median ratio of engine-child peak RSS.

| Workload | System puts/s | Mimalloc puts/s | Paired throughput | CPU | p99 | RSS ratio | Guards |
|---|---:|---:|---:|---:|---:|---:|---|
| Point / 1 writer(s) | 2,518 | 2,523 | +0.2% | -1.3% | -1.4% | 1.75x | Pass |
| Batch64 / 1 writer(s) | 82,337 | 100,750 | +22.4% | -27.1% | +5.6% | 1.07x | Pass |
| Point / 8 writer(s) | 16,929 | 16,972 | +1.0% | -5.7% | +5.7% | 1.48x | Pass |
| Batch64 / 8 writer(s) | 381,631 | 426,448 | +9.7% | -15.2% | -2.4% | 1.16x | Pass |
| Point / 16 writer(s) | 27,377 | 26,876 | -0.4% | -0.8% | +95.1% | 1.50x | Fail |
| Batch64 / 16 writer(s) | 547,316 | 550,857 | +2.5% | -3.9% | +1.1% | 1.15x | Pass |

## Same-cohort highest scores

These are the highest scored runs for each saved executable within this experiment. They exclude warmups and historical cohorts, and do not replace the paired assessment. The winner IDs, raw-record hashes and executable identities are retained in the [peak record](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-peaks.json).

| Workload | System v4 | Mimalloc v4 | System v7 | Mimalloc v7 | Unchanged retained v7 |
|---|---:|---:|---:|---:|---:|
| Point / 1 writer(s) | 2,523 | 2,539 | 2,250 | 2,260 | 2,263 |
| Batch64 / 1 writer(s) | 83,122 | 101,677 | 67,509 | 72,900 | 64,624 |
| Point / 8 writer(s) | 16,972 | 17,121 | 16,282 | 16,118 | 16,215 |
| Batch64 / 8 writer(s) | 388,833 | 454,896 | 416,438 | 353,801 | 418,674 |
| Point / 16 writer(s) | 27,855 | 30,013 | 23,889 | 25,100 | 24,695 |
| Batch64 / 16 writer(s) | 569,898 | 560,945 | 550,131 | 555,159 | 176,131 |

## Unchanged executable control

The System feature-off executable is compiled from the candidate source; the unchanged retained executable is an independent control. These paired changes compare System v7 with that unchanged reference. Moving SSD stalls remain in all arms; the large changes here limit causal interpretation of small allocator differences.

| Workload | Throughput | CPU | p99 | Control guards |
|---|---:|---:|---:|---|
| Point / 1 writer(s) | -0.6% | +1.3% | -0.0% | Pass |
| Batch64 / 1 writer(s) | +78.0% | -6.3% | -86.9% | Pass |
| Point / 8 writer(s) | -0.8% | +2.1% | +21.1% | Fail |
| Batch64 / 8 writer(s) | +3.3% | -5.5% | +1.5% | Pass |
| Point / 16 writer(s) | -44.6% | +19.4% | +497.5% | Fail |
| Batch64 / 16 writer(s) | +255.5% | -38.7% | -86.8% | Pass |

## Assessment failures

- `ext4-point8`: `v7_arm_median_floor`.
- `ext4-batch8`: `v7_paired_raw_floor`, `v7_arm_median_floor`, `v7_cpu_ceiling`, `v7_sync_count_ceiling`.
- `ext4-point16`: `v4_p99_ceiling`, `v7_paired_raw_floor`, `v7_arm_median_floor`, `v7_cpu_ceiling`.
- `ext4-batch16`: `v7_cpu_ceiling`, `v7_sync_count_ceiling`.

## Interpretation and final state

Single-writer v7 batch throughput gains 10.3% paired with 15.2% less CPU. At eight writers, v7 batch throughput instead falls 15.5%, CPU rises 56.8%, and sync count rises 11.9%. The highest scored batch8 mimalloc run is also 15.0% below the highest System run. At sixteen writers, paired batch throughput falls 3.0%, CPU rises 27.3%, and the sync-count ceiling fails, although the two highest throughputs are within 1%. The selected policy does not provide the requested consistent SSD benefit.

Point results are especially sensitive to the moving stalls. At sixteen writers, paired point throughput falls 43.0%, while the highest mimalloc run is 5.1% above the highest System run. System itself loses 44.6% paired against the unchanged executable and fails its latency/CPU control. At batch16, the unchanged executable's highest score is much lower than the paired System score. These failed or widely moving controls prevent treating every percentage as a precise causal allocator estimate. All observations remain; no slow runs are filtered and no scores are retried.

V7's median paired peak-RSS increase is 117.1% for point1, 44.5% for point8, 47.5% for point16, and 22.0–23.3% for the batch cells. The earlier native memory investigation already documents allocator reservation/retention and cross-thread behavior; this repeat measures the current child peaks without enabling runtime stats during scoring. It does not isolate the global allocator from the aligned-buffer switch, and it does not test newer upstream native versions.

All formats keep identical DATA byte counts within every pair. The increased v7 batch sync counts represent more durability groups, with the existing one-sync protocol unchanged. The counters do not by themselves distinguish allocator cost from cache, scheduling or grouping effects. This trial makes no changes to persisted hash functions, wire layout, recovery, publication, or I/O scheduling.

The four allocator source/manifest files are restored byte-for-byte, the canonical executable is atomically restored to the retained `e7f77128…` binary, and the trial's saved executables and exact patch remain available. A fresh outside-sandbox `cargo make check` on restored System passes all **1,969 tests**, with zero skips or retries. The runtime containing source commits `0ffebd51` and `71bd7c23` remains byte-identical to the experiment's source base. No allocator source commit, default change, PR or push is made. V4 parity and the complete Stage 8 gate remain unqualified.

## Evidence

There are 90 scored records, 30 warmups, one excluded profile record, and 1,453 clear process-audit samples. All 121 retained engine JSON records have unique IDs. The verifier reconstructs the 128 build inputs from the source base and exact patch, regenerates the summary, verifies the highest-score identities, and checks the local saved executables, validation logs, allocator proof and restoration. The ignored local executables/logs are required for those local hash checks. Disposable benchmark databases and reconstruction scratch are removed.

- [Frozen plan](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-plan.json)
- [Exact trial patch](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-candidate.patch)
- [Build-input hashes](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-source-sha256.tsv)
- [Saved executable identities](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-binaries.json)
- [Environment](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-environment.json)
- [Allocator activation proof](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-allocator-proof.json)
- [Excluded profile metadata](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-profile.json)
- [Resource-usage child launcher](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-child.py)
- [SSD driver](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-run.py)
- [Analyzer](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-analyze.py)
- [Full summary](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-summary.json)
- [Validation and correction record](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-checks.json)
- [Decision](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-decision.json)
- [Restoration](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-restoration.json)
- [Source/evidence verifier](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-verify.py)
- [Verification result](artifacts/rfc-025-v7-mimalloc-ssd-followup-20261027-verification.json)

Each cell has its raw JSON, manifest, process audit and exact wrapper:

| Cell | Raw | Manifest | Audit | Wrapper |
|---|---|---|---|---|
| ext4-point1 | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point1-20261027.jsonl) | [TSV](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point1-20261027-runs.tsv) | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point1-20261027-host.jsonl) | [shell](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point1-20261027-run.sh) |
| ext4-batch1 | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch1-20261027.jsonl) | [TSV](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch1-20261027-runs.tsv) | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch1-20261027-host.jsonl) | [shell](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch1-20261027-run.sh) |
| ext4-point8 | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point8-20261027.jsonl) | [TSV](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point8-20261027-runs.tsv) | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point8-20261027-host.jsonl) | [shell](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point8-20261027-run.sh) |
| ext4-batch8 | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch8-20261027.jsonl) | [TSV](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch8-20261027-runs.tsv) | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch8-20261027-host.jsonl) | [shell](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch8-20261027-run.sh) |
| ext4-point16 | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point16-20261027.jsonl) | [TSV](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point16-20261027-runs.tsv) | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point16-20261027-host.jsonl) | [shell](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-point16-20261027-run.sh) |
| ext4-batch16 | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch16-20261027.jsonl) | [TSV](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch16-20261027-runs.tsv) | [JSONL](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch16-20261027-host.jsonl) | [shell](artifacts/rfc-025-v7-mimalloc-ssd-followup-initial-ext4-batch16-20261027-run.sh) |

[WAL documentation](../README.md) · [RFC 025 implementation plan](../rfc-025-parallel-wal-embedded-frontiers-implementation-plan.md).
