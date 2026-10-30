# RFC 025: v7 preparation and batching experiments, 2026-10-25

**Result: retain preparation-aware bounded coalescing; v4 parity remains unmet.**

Across ten ext4 batch64 pairs, median raw throughput improves 6.8% with 2.8%
less CPU. Point-write guards remain mixed. Canonical-index skipping, partial
frame initialization, and a bounded batch-buffer pool are rejected and
reverted. All previous retained optimizations remain in place.

## Candidates and correctness

All four trials independently start from `51d2202d`, which contains the nine
previously retained optimizations, including FRONTIER encoding outside
admission (`f3ffb1c9`). They are never stacked together for a measurement.
The retained differential test is committed separately as `786a431e`, and the
retained coalescing optimization as `34a8b334`.

1. **Canonical-index skipping, rejected.** Strictly ordered point keys skip
   index allocation and its preparation charge. Unique unsorted points clear
   the index after duplicate detection. The one-entry path is unchanged. Batch
   throughput fails to improve on either filesystem, so the production change
   is reverted. A new independent differential test remains: all 9,330 mixed
   put, point-delete, and range-delete sequences of lengths one through five
   match the established v5 canonicalizer's exact retained order and bytes.
2. **Partial frame initialization, rejected.** Instead of clearing each frame
   completely before overwriting its payload, clear only the header and padding.
   Dirty-buffer and golden-codec tests pass, but paired throughput falls and CPU
   rises. The codec is restored byte-for-byte.
3. **Bounded batch-buffer pool, rejected.** Cache at most eight idle multi-frame
   buffers of at most 256 KiB each, separate from the single-frame pool. Exact
   active allocation charges, mismatch rejection, and terminal-CQE buffer pin
   retirement remain enforced. Reuse and mismatched-capacity tests pass, but
   the ext4 batch64 comparison uses 20.4% more CPU and has a lower peak. The
   entire runtime prototype and its prototype-specific tests are reverted.
4. **Preparation coalescing, retained.** When written tickets catch up with
   admission, query the reservation owner's existing batch-operation
   count. Excess operations may be unadmitted preparation or lingering DATA
   pins. This conservative hint can consume only the remaining existing
   coalescing deadline: 100 µs after fast v7 syncs, or 400 µs after slower
   syncs. It cannot move a written/durable boundary or extend that deadline.
   Closed admission ignores the hint. New tests check pending preparation,
   lingering pins, no premature durability, timeout, and shutdown. Wire bytes,
   the one-sync protocol, ticket ownership, and recovery are unchanged.

The rejected pool's median paired group count increases 39.8% on tmpfs and
29.6% on ext4; sync count increases 32.4% and 7.4%. This suggests producer
scheduling and batching can dominate allocator savings. These are correlated
counter changes, not an isolated causal attribution. They motivated the fourth
candidate's attempt to include preparation from the same producer wave.

## Measurement protocol

The baseline executable is `target/release/write-perf-v7-frontier-lock`, SHA-256
`5fad9ee139f12c9c5bff511750f4752e93a914f9fb716b1bc9954aea7323c6bb`.
Every candidate uses the same release build command:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

The host is the same Intel Core i9-13900T, Linux 6.12.71-1-lts, tmpfs and ext4
on the Solidigm SSDPFKNU010TZ. Each cell has four unscored warmups and five
alternating four-arm pairs: old/new binaries with v4 Parallel, and old/new
binaries with PITR v7 Parallel. Each run gets a fresh database, 1-KiB values,
and a 1-GiB SST target. Single-put cells issue 50,000 puts; batch64 cells issue
100,000 puts. Setup and close are outside the measured write interval.

Tmpfs records idle three seconds apart. SSD records have an initial 30-second
cooldown and ten seconds between records. No builds, tests, or profiling
capture overlap scored runs. The fourth candidate gets one fixed five-pair SSD
batch64 confirmation after its initial screen, followed by workload guards;
repeat selection does not depend on finding a new peak. The complete pass has
300 scored records and 60 warmups across 15 cells. All stalls and moving v4
controls remain recorded. No rejected peak is treated as a retained score.

Ratios below are medians of paired candidate/baseline ratios. Normalized
throughput first divides each v7 score by its same-binary v4 control. CPU is
process user plus system time. Ratios are not ratios of displayed medians.
Batch64 latency records have only 16 samples each; point records have 504 with
eight writers and 500 with one writer, so batch p50/p99 are especially sparse.

| Candidate / cell | Raw throughput | v4-normalized throughput | CPU | Sampled p99 |
| --- | ---: | ---: | ---: | ---: |
| Canonical index, tmpfs batch64 | 0.993x | 0.983x | 0.977x | 0.660x |
| Canonical index, ext4 batch64 screen | 0.959x | 0.873x | 0.984x | 1.048x |
| Canonical index, tmpfs point8 | 1.081x | 1.074x | 0.963x | 1.046x |
| Frame initialization, tmpfs batch64 | 0.916x | 0.933x | 1.083x | 0.898x |
| Frame initialization, ext4 batch64 screen | 0.950x | 0.927x | 1.026x | 1.031x |
| Frame initialization, tmpfs point8 | 0.979x | 0.989x | 1.031x | 0.931x |
| Batch pool, tmpfs batch64 | 1.048x | 1.137x | 1.056x | 0.870x |
| Batch pool, ext4 batch64 screen | 1.030x | 1.023x | 1.204x | 1.038x |
| Batch pool, tmpfs point8 | 0.991x | 1.033x | 1.021x | 0.897x |
| Preparation coalescing, tmpfs batch64 | 1.055x | 0.898x | 0.964x | 0.951x |
| Preparation coalescing, ext4 batch64 screen | 1.031x | 1.032x | 0.905x | 1.044x |
| Preparation coalescing, tmpfs point8 | 1.001x | 0.973x | 1.015x | 1.048x |
| Preparation coalescing, ext4 batch64 confirmation | 1.143x | 1.166x | 0.986x | 0.880x |
| Preparation coalescing, tmpfs point1 | 0.986x | 1.043x | 1.006x | 1.011x |
| Preparation coalescing, ext4 point8 | 1.011x | 0.980x | 0.969x | 0.623x |

The unchanged single-entry path in the canonical-index trial improves 8.1%
on tmpfs. That improvement cannot be attributed to the proposed index skip;
it is a control against overinterpreting the batch result. Similarly, partial
frame initialization reaches higher batch peaks despite losing paired
throughput and raising CPU. Peak-only selection would retain the wrong code.

Across ten SSD batch64 pairs, preparation coalescing improves raw throughput
6.8% and normalized throughput 10.7%, with 2.8% less CPU. Sampled p50 falls
10.2%, while sampled p99 is almost unchanged (`0.997x`); sparse samples limit latency
conclusions. The initial and confirmation median sync-count ratios are
`0.962x` and `0.977x`, consistent with somewhat better sync batching. These
measurements do not attribute every throughput change to that counter.

Tmpfs batch64 raw throughput improves 5.5% with 3.6% less CPU, but its normalized
ratio falls 10.2% as the v4 controls move. Tmpfs point8 is nearly flat raw,
with a 2.7% lower normalized ratio and 4.8% higher sampled p99. Solo tmpfs raw
throughput falls 1.4%, its normalized ratio improves 4.3%, and CPU/p99 rise
0.6%/1.1%. These mixed controls prevent a uniform no-regression claim.

The final ext4 point-write guard improves raw throughput 1.1% with 3.1%
less CPU, but its normalized ratio falls 2.0%. Sampled p50/p99 ratios are
`0.978x`/`0.623x`; SSD stalls make the tail result especially variable.
Preparation-aware bounded coalescing is retained for its repeat batch gain,
with these point and control limits recorded. The last SSD guard is not
rerun to search for a better score.

## Highest comparable scored results

Warmups and rejected candidates are excluded. Stronger historical v4 and
retained v7 highs stay visible alongside this pass's matched baseline.

| Build / format | Single puts, 8 writers | Batch64, 8 writers |
| --- | ---: | ---: |
| Highest comparable prior v4 Parallel | 20,420 | 482,410 |
| Highest prior retained v7 extent cache | 15,759 | 391,992 |
| FRONTIER-lock baseline in this pass | 15,729 | 389,552 |
| Preparation-coalescing candidate | 15,664 | 390,163 |

The candidate peaks remain 23.3% and 19.1% below the strongest v4 references.
They also remain 0.6% and 0.5% below the stronger prior retained v7 highs.
This pass improves paired batch behavior without establishing a new
historical v7 peak, v4 parity, or the Stage 8 performance gate. The full
workload and operational-recovery qualification remains open.

Temperatures across the retained candidate's SSD comparisons range from
39.85 to 45.85 °C. These readings do not establish the absence of storage stalls.

## Validation and source provenance

Every prototype passes `cargo make check` outside the io_uring sandbox,
including formatting, dependency ordering, all-feature/all-target Clippy,
unused-dependency checks, nextest, and typos:

| Candidate | Tests passed | Nextest run | Qualification notes |
| --- | ---: | --- | --- |
| Canonical index | 1,959 | `1c22e3e5-8102-4676-95f5-29cd85e3dd22` | One unchanged leveled-compaction test takes 32.4 s; no retry |
| Partial frame initialization | 1,959 | `50190fcb-35d4-4c0c-85e0-44b9c30555a0` | One unchanged leveled-compaction test takes 32.4 s; no retry |
| Batch-buffer pool | 1,960 | `c4899927-d08a-4282-98ad-50a5f1a974a7` | Zero skipped/retried tests |
| Preparation coalescing | 1,961 | `feadfa9c-f704-4842-813c-633480ec1654` | One unrelated simple-leveled compaction timeout passes on retry; one leveled-compaction test takes 32.4 s |

The compaction fixtures use WAL-disabled test options. The preparation hint's
initial new test fixture omitted an explicit sync after injecting CQEs; the
fixture was corrected to follow the existing manually driven coordinator
pattern before the successful full check. This was a deterministic fixture
failure, not a discarded performance run. Logs remain in the per-candidate
`target/wal-opt/20261025-v7-*` directories. The candidate's final check log is
`target/wal-opt/20261025-v7-preparation-coalesce/check.log`.

When restoring a prototype, preserve fresh source modification times so Cargo
rebuilds the correct source. An initial pool-restore check reused a cached
prototype binary because `copy2` restored old timestamps; that run is excluded.
The sources were touched and the full check rerun: 1,959 tests passed, zero
skips/retries, nextest `b2706710-5797-4a81-a87d-a6c949718ea8`. Subsequent candidate
checks rebuilt after that verified restore.

- **canonical-index**: executable SHA-256 `7a5a9117cf3b02e94e7ae39a737d0b946c1afae06c30e4d19d015c2a0b15d417`.
  [Source manifest](artifacts/rfc-025-v7-canonical-index-20261025-source-sha256.tsv),
  [source patch](artifacts/rfc-025-v7-canonical-index-20261025-rejected.patch),
  [analyzer](artifacts/rfc-025-v7-canonical-index-20261025-analyze.py), and
  [summary](artifacts/rfc-025-v7-canonical-index-20261025-summary.json).
- **frame-init**: executable SHA-256 `7b36faadd9352d702a6661cd52f88ae9522da8b7163f7aa64e0acd2777b0cc2c`.
  [Source manifest](artifacts/rfc-025-v7-frame-init-20261025-source-sha256.tsv),
  [source patch](artifacts/rfc-025-v7-frame-init-20261025-rejected.patch),
  [analyzer](artifacts/rfc-025-v7-frame-init-20261025-analyze.py), and
  [summary](artifacts/rfc-025-v7-frame-init-20261025-summary.json).
- **batch-pool**: executable SHA-256 `025b3e534ce3770eaba1fbf3a0601106f11eb6234f4250415f009b70c4db00d8`.
  [Source manifest](artifacts/rfc-025-v7-batch-pool-20261025-source-sha256.tsv),
  [source patch](artifacts/rfc-025-v7-batch-pool-20261025-rejected.patch),
  [analyzer](artifacts/rfc-025-v7-batch-pool-20261025-analyze.py), and
  [summary](artifacts/rfc-025-v7-batch-pool-20261025-summary.json).
- **preparation-coalesce**: executable SHA-256 `b7418dda91701807a76809a094eaa0538540efcac9a1cb0f683881f9ad02ae34`.
  [Source manifest](artifacts/rfc-025-v7-preparation-coalesce-20261025-source-sha256.tsv),
  [source patch](artifacts/rfc-025-v7-preparation-coalesce-20261025-candidate.patch),
  [analyzer](artifacts/rfc-025-v7-preparation-coalesce-20261025-analyze.py), and
  [summary](artifacts/rfc-025-v7-preparation-coalesce-20261025-summary.json).

The source manifests each cover 128 Rust/build inputs. Reconstruct the first
candidate from its complete rejected patch on `51d2202d`. For frame-init and
pool, first apply the [retained differential-test patch](artifacts/rfc-025-v7-canonical-sequences-20261025-test.patch),
then the respective rejected patch. The coalescing candidate patch includes
the differential test and applies directly to the same baseline. All patches
use zero context; use `git apply --unidiff-zero`. Exact reconstructed input
hashes were checked against every row of all four source manifests.

## Reproduction artifacts

| Trial / cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| canonical-index, tmpfs-batch64-8writers | [script](artifacts/rfc-025-v7-canonical-index-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-canonical-index-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-canonical-index-tmpfs-batch64-8writers-20261025.jsonl) |
| canonical-index, ext4-batch64-8writers | [script](artifacts/rfc-025-v7-canonical-index-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-canonical-index-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-canonical-index-ext4-batch64-8writers-20261025.jsonl) |
| canonical-index, tmpfs-8writers | [script](artifacts/rfc-025-v7-canonical-index-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-canonical-index-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-canonical-index-tmpfs-8writers-20261025.jsonl) |
| frame-init, tmpfs-batch64-8writers | [script](artifacts/rfc-025-v7-frame-init-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frame-init-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frame-init-tmpfs-batch64-8writers-20261025.jsonl) |
| frame-init, ext4-batch64-8writers | [script](artifacts/rfc-025-v7-frame-init-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frame-init-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frame-init-ext4-batch64-8writers-20261025.jsonl) |
| frame-init, tmpfs-8writers | [script](artifacts/rfc-025-v7-frame-init-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-frame-init-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-frame-init-tmpfs-8writers-20261025.jsonl) |
| batch-pool, tmpfs-batch64-8writers | [script](artifacts/rfc-025-v7-batch-pool-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-batch-pool-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-pool-tmpfs-batch64-8writers-20261025.jsonl) |
| batch-pool, ext4-batch64-8writers | [script](artifacts/rfc-025-v7-batch-pool-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-batch-pool-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-pool-ext4-batch64-8writers-20261025.jsonl) |
| batch-pool, tmpfs-8writers | [script](artifacts/rfc-025-v7-batch-pool-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-batch-pool-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-batch-pool-tmpfs-8writers-20261025.jsonl) |
| preparation-coalesce, tmpfs-batch64-8writers | [script](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-batch64-8writers-20261025.jsonl) |
| preparation-coalesce, ext4-batch64-8writers | [script](artifacts/rfc-025-v7-preparation-coalesce-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-preparation-coalesce-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-preparation-coalesce-ext4-batch64-8writers-20261025.jsonl) |
| preparation-coalesce, tmpfs-8writers | [script](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-8writers-20261025.jsonl) |
| preparation-coalesce, confirm-ext4-batch64-8writers | [script](artifacts/rfc-025-v7-preparation-coalesce-confirm-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-preparation-coalesce-confirm-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-preparation-coalesce-confirm-ext4-batch64-8writers-20261025.jsonl) |
| preparation-coalesce, tmpfs-1writer | [script](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-1writer-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-1writer-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-preparation-coalesce-tmpfs-1writer-20261025.jsonl) |
| preparation-coalesce, ext4-8writers | [script](artifacts/rfc-025-v7-preparation-coalesce-ext4-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-preparation-coalesce-ext4-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-preparation-coalesce-ext4-8writers-20261025.jsonl) |

The retained executable is `target/release/write-perf-v7-preparation-coalesce`;
the default `target/release/write-perf` has the same candidate hash. The retained
source still matches all 128 rows of the recorded candidate manifest.

Each driver refuses to overwrite records and deletes disposable databases.
The analyses verify raw-record and binary hashes, workload settings, sample
counts, and five complete four-arm pairs; their summaries retain every best
run ID and pipeline counter. Run each linked analyzer from the repository to
reproduce its summary. All previous retained optimizations remain in place.

[WAL documentation](../README.md).
