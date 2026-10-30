# RFC 025: BBTree point-memtable experiment, 2026-10-27

**Result: point reads improve 10.8%, but sixteen-writer publication loses 25.5% and fails the fixed CPU screen. The prototype is reverted; all planned SSD cells are cancelled before execution. SkipMap, System, and the retained SSD scheduling improvements remain selected.**

The maintainer requested trying [BBTree](https://bbtree.xiangpeng.systems/post/). The [author’s text article](https://blog.xiangpeng.systems/posts/bbtree/) describes replacing LSM memtable skiplists. This experiment starts at `c6d4c3526fa234ac308c6509fdf36cbede20cd1e`, following the allocator retry, and tests the published `bbtree 0.1.5` crate (upstream source `ff4f37ca199220452d65005742596635ee8fb621`). Its locked registry checksums and verification dependencies are preserved in the upstream record.

## Integration scope

The optional `bbtree-memtable` feature replaces point-entry storage with the concurrent `BBTreeMap`. Default builds retain SkipMap. A newtype wraps existing `Bytes` keys and delegates byte-string inline windows, prefix comparisons and hash tags to BBTree’s `[u8]` implementation, preserving shared key ownership and full internal-key ordering. Values remain `Bytes`. This is an in-memory index change, with System retained and no change to WAL/SST encodings, hashes, durability barriers, publication frontiers or I/O scheduling. Range tombstones and transaction-local maps remain on their existing skiplists.

Versioned lookups use a leaf-scoped scan that stops at the first visible matching version, without allocating a range cursor. Public iterators retain only owned byte references and bounded read-ahead of up to 64 entries; they hold no leaf lock between calls and resume after the last full internal key. MVCC callers retain their fixed read timestamp. The engine iterator remains Send without adding unsafe Send implementations. Flush and vLog resolution consume the same raw values and internal keys.

Legacy WAL replay uses its existing compatibility SkipMap, then imports those entries into BBTree; v7 replay already inserts through the memtable. This extra legacy-recovery workspace is a prototype limitation and is not measured here. Out-of-memory during publication remains fail-stop, so a durable batch is not turned into a recoverable partial-publication error. Each publication batch still calls individual tree inserts; this trial does **not** use BBTree’s native `insert_many` API or evaluate sharded trees.

## Protocol and measurement limits

The plan and exact matrix are frozen before qualifying captures. The immediately preceding excluded user-CPU baseline profile is reused because the retained engine source and exact `e7f77128…` executable are unchanged. It has 596 samples, zero lost samples, and visible SkipMap search work. It measures neither kernel CPU nor off-CPU waits.

The CPU screen uses existing engine workloads: 100,000 published entries in batches of 64 at 1/8/16 writers; 500,000 successful random reads over 100,000 loaded keys; and full / 10% scans of 100,000 keys. All use 1-KiB values and a 2-GiB SST target. Measured reads/scans remain below the freeze threshold. They have no WAL; loading and close are outside each measured phase, and close may write SSTs. These are not SSD durability measurements.

Each cell has one warmup per backend and exactly three alternating scored pairs, with a three-second initial and inter-command cooldown. Source and executables remain frozen, with no overlapping builds, tests or profiles. Recognized competing processes are audited before, once per second during, and after commands; 133 samples are clear. This sampling does not exclude every unrelated or brief task.

Throughput covers the engine’s measured phase. Linux wait4 CPU and peak RSS cover the exact child’s whole lifetime, including load and close. They exclude the launcher’s high-water mark. Both scan phases share one invocation and its resource usage; their CPU/RSS rows do not represent separate phase measurements. RSS here does not isolate tree-node storage or prove long-running memory retention. No p99 measurement is available for these CPU workloads.

CPU screening requires paired and separate-arm median throughput of at least 0.90x for every phase. Only a passing CPU screen advances to v4/v7 Parallel SSD point8 and batch8; the frozen plan then permits 1/16-writer SSD follow-ups if that screen passes. Material gains do not waive a failed concurrency guard. All completed scored observations remain, with no score-selected retry or filtering.

## CPU results

Changes are medians of three within-pair BBTree/SkipMap ratios. The throughput columns are separate arm medians, whose quotient can differ from the paired median. CPU and RSS are whole-child measurements.

| Workload | SkipMap ops/s | BBTree ops/s | Paired throughput | Child CPU | Peak RSS | Screen |
|---|---:|---:|---:|---:|---:|---|
| Publish / 1 writer | 936,435 | 1,010,523 | +8.3% | -6.7% | +5.2% | Pass |
| Publish / 8 writers | 2,917,835 | 3,258,758 | +2.9% | +3.7% | +5.9% | Pass |
| Publish / 16 writers | 3,414,018 | 2,541,751 | -25.5% | +31.2% | +5.7% | Fail |
| Point get | 441,740 | 489,457 | +10.8% | -10.7% | +1.3% | Pass |
| Full scan (rows/s) | 1,303,288 | 1,315,529 | +0.9% | +1.1% | +1.3% | Pass |
| 10% scan (rows/s) | 1,420,287 | 1,396,012 | -1.6% | +1.1% | +1.3% | Pass |

The sixteen-writer failure appears in both the paired ratio and separate-arm medians. Even its highest BBTree publication score is below the highest SkipMap score. Read gains are useful evidence, but they do not qualify this prototype for adoption or SSD testing.

## Highest scores within this cohort

These exclude warmups, profiles and prior cohorts. They are diagnostic, not substitutes for the paired screen. Winning invocation IDs, raw hashes and binary hashes are recorded in the peak record.

| Workload | SkipMap highest | BBTree highest | Change |
|---|---:|---:|---:|
| Publish / 1 writer | 1,046,417 | 1,032,453 | -1.3% |
| Publish / 8 writers | 3,165,580 | 3,268,483 | +3.3% |
| Publish / 16 writers | 3,551,044 | 2,949,751 | -16.9% |
| Point get | 443,414 | 494,154 | +11.4% |
| Full scan (rows/s) | 1,336,519 | 1,322,138 | -1.1% |
| 10% scan (rows/s) | 1,459,389 | 1,436,350 | -1.6% |

## Excluded contention investigation

After scoring, one larger diagnostic per backend publishes one million entries with sixteen writers. Both are explicitly excluded from the screen. Perf records `cpu-clock:u` at 99 Hz with DWARF call stacks: 117 SkipMap samples and 189 BBTree samples, with zero lost samples. The larger dataset and profiling perturbations prevent pooling these with the 100,000-entry scored cells.

BBTree spends 37.04% of its flat user-CPU samples in `NodeLatch::read_version_contended`, versus no BBTree latch in the SkipMap executable. SkipMap search accounts for 14.53% of its own samples. This is evidence of tree contention in this workload; it does not identify which node level causes every wait, measure kernel/off-CPU time, or prove that all measured regression comes from this function. Native batched insertion or another tree layout would require a separate prototype and fixed measurements.

## Checks, restoration and evidence

The selected backend passes cargo make check: **1,973 tests**, four more than the original suite, with zero skips/retries and one slow compaction integration test. Additional tests cover binary/zero/shared-prefix key ordering against an ordered model, precise range bounds across refills, writes with a live cursor and held entry, Send compatibility, and MVCC version visibility across leaf splits. Existing recovery, TTL, range-tombstone, checkpoint, vLog and crash tests exercise the selected backend. Nextest with the original feature set, excluding `bbtree-memtable`, passes all **1,969 tests**. Formatting, default/all-feature Clippy, dependency and spelling checks pass for the prototype.

The three original manifests/source files are restored byte-for-byte, the added trial module is removed, and the canonical executable is atomically restored to `e7f7712825d2166c14b5b67601388a8e29697f8fc034832e457061cf6e30ae19`. Restored Rust checks pass all **1,969 tests**. The runtime containing the async and serial SSD fixes stays byte-identical. Saved trial executables and the exact source patch remain for review; no backend source commit, allocator switch, PR or push is made.

There are **36 scored JSON records**, **12 warmup records**, and **40 completed scored/warmup invocations**. Scans produce two records per invocation. Two additional excluded diagnostic invocations make **42 retained unique invocation IDs**. An earlier unscored warmup completed before a harness label error prevented its JSON/audit persistence; its raw stdout was lost, its remaining resource/stderr files are retained, and it is counted separately rather than as a valid capture. The harness was corrected before any scored capture.

The initial source diff was also rendered by the repository’s external side-by-side diff tool. It was replaced with an applicable unified patch generated from the frozen source snapshots; source hashes, executables and measurements are unchanged. Restored cargo-make returned success while its spelling output exposed this rendered-patch problem. A final direct typos check validates the corrected evidence. Both corrections are recorded explicitly.

The verifier reconstructs 129 build inputs, regenerates the summary, checks exact executable and log hashes, validates cancellation and restoration, and confirms that temporary benchmark databases are gone. Its local executable/profile/log hash checks require the ignored saved files under target. The complete RFC performance gate and v4 parity remain unqualified; no SSD result is inferred from this CPU screen.

- [Frozen plan](artifacts/rfc-025-bbtree-memtable-20261027-plan.json)
- [Exact matrix](artifacts/rfc-025-bbtree-memtable-20261027-matrix.json)
- [Trial patch](artifacts/rfc-025-bbtree-memtable-20261027-candidate.patch)
- [129 source hashes](artifacts/rfc-025-bbtree-memtable-20261027-source-sha256.tsv)
- [Upstream identities](artifacts/rfc-025-bbtree-memtable-20261027-upstream.json)
- [Executable identities](artifacts/rfc-025-bbtree-memtable-20261027-binaries.json)
- [Environment](artifacts/rfc-025-bbtree-memtable-20261027-environment.json)
- [Driver](artifacts/rfc-025-bbtree-memtable-20261027-run.py)
- [Exact-child resource collector](artifacts/rfc-025-bbtree-memtable-20261027-child.py)
- [Analyzer](artifacts/rfc-025-bbtree-memtable-20261027-analyze.py)
- [Summary](artifacts/rfc-025-bbtree-memtable-20261027-summary.json)
- [Highest-score identities](artifacts/rfc-025-bbtree-memtable-20261027-peaks.json)
- [Diagnostic plan](artifacts/rfc-025-bbtree-memtable-20261027-profiles-plan.json)
- [Diagnostic raw records](artifacts/rfc-025-bbtree-memtable-20261027-profiles.jsonl)
- [Diagnostic metadata and audits](artifacts/rfc-025-bbtree-memtable-20261027-profiles.json)
- [Flat profile sample shares](artifacts/rfc-025-bbtree-memtable-20261027-profile-samples.json)
- [Checks](artifacts/rfc-025-bbtree-memtable-20261027-build-checks.json)
- [Harness correction](artifacts/rfc-025-bbtree-memtable-20261027-harness-correction.json)
- [Patch representation correction](artifacts/rfc-025-bbtree-memtable-20261027-patch-correction.json)
- [Cancellation decision](artifacts/rfc-025-bbtree-memtable-20261027-decision.json)
- [Restoration](artifacts/rfc-025-bbtree-memtable-20261027-restoration.json)
- [Source/evidence verifier](artifacts/rfc-025-bbtree-memtable-20261027-verify.py)
- [Verification result](artifacts/rfc-025-bbtree-memtable-20261027-verification.json)

| Cell | Raw | Manifest | Audit |
|---|---|---|---|
| cpu-publish1 | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish1.jsonl) | [TSV](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish1-runs.tsv) | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish1-host.jsonl) |
| cpu-publish8 | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish8.jsonl) | [TSV](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish8-runs.tsv) | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish8-host.jsonl) |
| cpu-publish16 | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish16.jsonl) | [TSV](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish16-runs.tsv) | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-publish16-host.jsonl) |
| cpu-read | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-read.jsonl) | [TSV](artifacts/rfc-025-bbtree-memtable-20261027-cpu-read-runs.tsv) | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-read-host.jsonl) |
| cpu-scan | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-scan.jsonl) | [TSV](artifacts/rfc-025-bbtree-memtable-20261027-cpu-scan-runs.tsv) | [JSONL](artifacts/rfc-025-bbtree-memtable-20261027-cpu-scan-host.jsonl) |

[WAL documentation](../README.md) · [RFC 025 implementation plan](../rfc-025-parallel-wal-embedded-frontiers-implementation-plan.md).
