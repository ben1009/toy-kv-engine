# RFC 025: borrowing PITR maintenance state, 2026-10-25–26

**Result: rejected; the preceding source and executable are restored.**

Across ten ext4 point-write pairs, median raw throughput improves
1.2%, the v4-normalized ratio improves 1.0%, and CPU falls
3.2%. The batch regression below prevents retention. All
controls and the stronger historical peaks remain visible below.

## Change and correctness

Every PITR commit checks whether size, time, or pending work requires
maintenance. Previously that check cloned the complete persisted `PitrState`,
including its obligation map, then discarded most fields. The prototype reads
only the scheduling coordinates and inspects the obligations in place.

The PITR mutex protects the mode, two configuration scalars, active segment ID,
and obligation predicate. One storage snapshot supplies the active and frozen
memtables for that predicate. The mutex is explicitly released before querying
WAL length, locking the timer, or dispatching background work. The storage
snapshot remains alive beyond the mutex, so its destruction also occurs after
the mutex is released. No new cache, atomic, timer, or public configuration is
introduced. These are scheduling hints; the maintenance worker retains its
existing lifecycle and segment revalidation.

The predicate still schedules publication-uncertain state and non-reclaimable
obligations. A reclaimable source is pending cleanup once its memtable is no
longer live; active/frozen sources remain pinned as before. Empty successors
can still schedule pending cleanup. Existing size/interval conditions, queue
arbitration, error paths, and close fences remain in force. Legacy PITR writers
also avoid the clone. WAL encoding, checksums, hashes, sync ordering, and
persisted formats are unchanged.

The existing maintenance tests cover size and timer rotation, idle successors,
cleanup retry, reclaimable sources, interrupted migration, and close fencing.
The prototype passes all 1,963 tests with zero skips/retries/slow tests. Nextest run:
`bd9a6914-b49a-4731-9374-8ce6331960fc`. Formatting, dependency ordering,
all-feature/all-target Clippy, unused-dependency checks, and release build pass.
The full log is `target/wal-opt/20261025-v7-maintenance-snapshot/check.log`.
The typo task initially reported TinyUFO's external `FiFoQueues` type name in
a captured profile while returning success; direct `typos` was used to check
the fix. A narrow identifier exception preserves the profile verbatim, and
direct `typos` now passes. No profile symbols are rewritten.

After rejection, `lsm_storage.rs` is restored byte-for-byte to `b13bbe57`,
with its modification time refreshed before rebuilding the checks. A fresh
`cargo make check` passes all 1,963 tests, with zero skips/retries and one slow
existing leveled-compaction integration test (32.5 seconds). Nextest run:
`bafc39af-99de-451a-83f7-13b26d8bd77d`. The restoration log is
`target/wal-opt/20261025-v7-maintenance-snapshot/restored-check.log`.
The default `target/release/write-perf` is restored to the retained
immutable-batch executable, SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
No Rust performance change from this experiment remains.

## Diagnosis and protocol

Fresh unscored profiles pin the preceding immutable-batch source `c5b344a5`
and executable `target/release/write-perf-v7-validated-logical-batch`, SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
The point profile samples maintenance dispatch at 1.81% self / 2.51% inclusive,
and obligation-map cloning at 1.16% inclusive. The trace and source motivated
removing repeated state copies; these percentages do not attribute every clone
to one caller or quantify its throughput effect. Batch SHA-256 remains the
largest sampled cost, at 20.62% on the ordered packer.

Profiles include setup and close; v4 also flushes its memtable at close, outside
the measured write interval. Their three runs are excluded from scoring. No
profiles, builds, or checks overlap scored comparisons.

The candidate is `target/release/write-perf-v7-maintenance-snapshot`, SHA-256
`91a5aa113886f2f91e575360c212ab0ac9c27a41835be4c49fea038a03c3a7aa`.
Its [128-input source manifest](artifacts/rfc-025-v7-maintenance-snapshot-20261025-source-sha256.tsv)
has SHA-256 `2b15af4859d2d72b217a975bc2caf5a5c532d70244b41d47716ef0fbe17a1b13`.
The complete [source patch](artifacts/rfc-025-v7-maintenance-snapshot-20261025-rejected.patch)
applies to `b13bbe57` with `git apply --unidiff-zero`; the profile spelling
exception is separate from the Rust change. Both binaries use:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

The host remains an Intel Core i9-13900T, Linux 6.12.71-1-lts, tmpfs and ext4
on a Solidigm SSDPFKNU010TZ. Each cell has four unscored warmups and five
alternating four-arm pairs: baseline/candidate binaries with ordinary v4
Parallel, and baseline/candidate with PITR v7 Parallel. Each run gets a fresh
database, 1-KiB values, and a 1-GiB SST target. Point runs issue 50,000 puts;
batch64 runs issue 100,000 puts. Setup and close are outside the timed interval.
Tmpfs records idle three seconds apart; ext4 uses an initial 30-second cooldown
and ten seconds between records.

This experiment targets point writes. A promising screen gets one fixed
five-pair SSD point confirmation, followed by SSD batch64 and solo tmpfs
guards before retention. Further SSD repeats are not selected by a peak.
All stalls and moving v4 controls remain included. Filenames retain the date
on which this experiment started; it continued into October 26 local time.

Ratios below are medians of paired candidate/baseline ratios. The normalized
ratio divides each v7 score by its same-binary v4 control before comparing
binaries. CPU is measured process user plus system time. Batch64 has only
16 latency samples per run; point writes have 504 with eight writers and
500 with one writer, so batch latency is particularly sparse.

| Cell | Raw throughput | v4-normalized throughput | CPU | Sampled p99 |
| --- | ---: | ---: | ---: | ---: |
| tmpfs, point8 | 1.063x | 1.016x | 0.954x | 0.642x |
| tmpfs, batch64 | 1.038x | 1.048x | 0.956x | 1.067x |
| ext4, point8 screen | 1.022x | 1.004x | 0.974x | 0.999x |
| ext4, point8 confirmation | 1.003x | 1.016x | 0.954x | 1.020x |
| ext4, batch64 guard | 0.962x | 0.929x | 0.969x | 1.017x |
| tmpfs, point1 guard | 1.094x | 0.973x | 0.917x | 0.985x |
| ext4, batch64 confirmation | 0.922x | 0.994x | 1.148x | 0.953x |

The confirmation alone changes point throughput by `1.003x` raw and `1.016x`
normalized, with `0.954x` CPU. Across all ten SSD point pairs, sampled p50 and
p99 ratios are `0.994x` / `1.009x`. No additional point
repeat is selected to improve the result or peak.

Tmpfs point8 improves 6.3% raw and 1.6% normalized, with 4.6% less CPU. Tmpfs
batch64 improves 3.8% raw and 4.8% normalized, with 4.4% less CPU, but its
sparse sampled p50/p99 rise 5.6%/6.7%. The pooled SSD batch64 comparison changes raw
throughput -5.8%, normalized throughput -3.8%, and CPU
+3.6%. Solo tmpfs changes raw throughput +9.4%, normalized
throughput -2.7%, and CPU -8.3%. These controls limit any
uniform no-regression claim.

There are 140 scored records and 28 warmups across seven completed cells, plus three
unscored profiles. Every stall and unfavorable latency/control result is
retained. The change is rejected under the stated batch guard. The eleven earlier
optimizations remain; this does not qualify the Stage 8 gate.

The first SSD batch guard changes raw throughput -3.8%
and normalized throughput -7.1%, triggering one fixed additional
five-pair batch comparison. This follow-up investigates an observed regression;
it was added after the initial guard and before its confirmation ran. The
retention rule required both pooled raw and normalized batch medians to reach
baseline. Across ten pairs, batch raw throughput changes
-5.8%, normalized throughput -3.8%, and CPU
+3.6%. All initial and confirmation records are kept. No further
repeats follow this decision.

## Highest comparable scored results

| Build / format | Point puts/s, 8 writers | Batch64 puts/s, 8 writers |
| --- | ---: | ---: |
| Strongest prior v4 Parallel | 20,420 | 482,410 |
| Strongest prior retained v7 | 15,759 | 391,992 |
| Immutable-batch baseline in this pass | 15,398 | 371,799 |
| Borrowed maintenance candidate | 15,376 | 359,252 |

The candidate is 24.7% below the strongest v4 point reference and
25.5% below its batch64 reference. These are maxima across different runs;
paired comparisons retain the contemporaneous v4 controls. Earlier v7
highs are preserved as separate references, rather than replaced by this
pass's slower controls. V4 parity remains unproven.

## Reproduction artifacts

The [analyzer](artifacts/rfc-025-v7-maintenance-snapshot-20261025-analyze.py)
checks raw/binary hashes, settings, counts, and complete pairs and reproduces
the [summary](artifacts/rfc-025-v7-maintenance-snapshot-20261025-summary.json).
Winning run IDs, pipeline counters, and temperatures remain included.
All seven drivers pass shell syntax checks; all 171 run IDs are distinct.
Reconstructing the rejected patch against `b13bbe57` reproduces all 128 source
hashes. The summary regenerates exactly from the committed records.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs, point8 | [script](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-8writers-20261025.jsonl) |
| tmpfs, batch64 | [script](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-batch64-8writers-20261025.jsonl) |
| ext4, point8 screen | [script](artifacts/rfc-025-v7-maintenance-snapshot-ext4-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-maintenance-snapshot-ext4-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-maintenance-snapshot-ext4-8writers-20261025.jsonl) |
| ext4, point8 confirmation | [script](artifacts/rfc-025-v7-maintenance-snapshot-confirm-ext4-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-maintenance-snapshot-confirm-ext4-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-maintenance-snapshot-confirm-ext4-8writers-20261025.jsonl) |
| ext4, batch64 guard | [script](artifacts/rfc-025-v7-maintenance-snapshot-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-maintenance-snapshot-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-maintenance-snapshot-ext4-batch64-8writers-20261025.jsonl) |
| tmpfs, point1 guard | [script](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-1writer-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-1writer-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-maintenance-snapshot-tmpfs-1writer-20261025.jsonl) |
| ext4, batch64 confirmation | [script](artifacts/rfc-025-v7-maintenance-snapshot-confirm-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-maintenance-snapshot-confirm-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-maintenance-snapshot-confirm-ext4-batch64-8writers-20261025.jsonl) |

Unscored CPU profiles: [driver](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025-run.py),
[commands/hashes](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025-runs.tsv),
and [JSONL](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025.jsonl).

| Diagnostic cell | Flat samples | Call paths |
| --- | --- | --- |
| v7 batch64 | [report](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025-v7-batch64-flat.txt) | [report](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025-v7-batch64-calls.txt) |
| v7 point | [report](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025-v7-point-flat.txt) | [report](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025-v7-point-calls.txt) |
| v4 point | [report](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025-v4-point-flat.txt) | [report](artifacts/rfc-025-v7-immutable-batch-cpu-profile-20261025-v4-point-calls.txt) |

Only display padding is trimmed from profile text. Captures and logs remain
under `target/wal-opt/20261025-v7-maintenance-snapshot`. Disposable databases
are removed after each run. All eleven earlier retained optimizations remain.

[WAL documentation](../README.md).
