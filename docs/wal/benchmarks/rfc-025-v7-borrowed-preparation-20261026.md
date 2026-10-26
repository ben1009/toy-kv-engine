# RFC 025: borrowed v7 producer preparation, 2026-10-26

**Result: rejected; the retained source and executable are restored. V4 parity remains unmet.**

Across ten ext4 batch64 pairs, raw throughput changes -3.1%,
the v4-normalized ratio changes -10.0%, and CPU changes
+2.9%. The ext4 point guard changes raw throughput +2.5%
and normalized throughput +4.1%.

## Change and correctness

Point writes previously cloned a key and prefixed value into a temporary
`WalBatch`. Standard synchronous point batches similarly allocated owned keys,
prefixed values, and an entry vector before encoding. The prototype borrows a repeatable iterator
of entries and writes directly into the charged, encoder-owned logical buffer.
Raw PUT values receive the existing inline tag during encoding. Publication
still owns its own key/value bytes.

The canonicalizer keeps its bounded, charged point-operation index and retains
the final point operation per key in the original surviving order. Range
operations preserve their original position. Input limits apply before the
index allocation; key/value and byte limits apply to canonical survivors. Raw
value limits include the inline tag. The owned-input adapter uses the same
validator and writer. V5/v6 wire layouts and golden fixtures remain unchanged.

Only preparation borrows source data. The returned prepared request owns its
encoded bytes and buffers before timestamp ordering and can outlive or release
the producer inputs. The existing reservation owner, preflight capacity
rechecks, cancellation cleanup, seal index, timestamp assignment, poisoning,
DATA framing, hashes, and FRONTIER sync ordering remain intact. No new queue,
cache, format, or runtime configuration is introduced.

New tests compare all 1,554 mixed borrowed entry sequences against the
independent owned v5 fixture, check exact raw-value and canonicalization limits,
and compare complete logical batches and DATA frames at fragment boundaries.
The framing test drops its source before finishing ordered metadata. Existing
pinned byte vectors, runtime resource/cancellation tests, recovery tests, and
PITR lifecycle tests also run.

The prototype passes all 1,966 tests with zero skips/retries/slow tests. Nextest run:
`d0c8dc04-c6bd-4689-9055-464e533ccadf`. The full `cargo make check` passes
formatting, dependency sorting, all-target/all-feature Clippy, unused-dependency
checks, tests, and spelling. Release build passes. Logs remain under
`target/wal-opt/20261026-v7-borrowed-preparation`. An earlier compile check
caught a missing test-only owned-input adapter, which was restored before the
successful suite and benchmark build.

After rejection, all six Rust files are restored byte-for-byte to `3ad46356`,
with refreshed modification times before checking. A fresh `cargo make check`
passes all 1,963 retained-source tests with zero skips/retries/slow tests.
Nextest run: `66fa979a-8211-4675-a2b8-8b36171ee0e5`. The restoration log is
`target/wal-opt/20261026-v7-borrowed-preparation/restored-check.log`.
The default `target/release/write-perf` is restored to SHA-256
`614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
No Rust performance change from this experiment remains.

## Diagnosis and fixed protocol

Three fresh unscored ext4 profiles use the retained immutable-batch executable,
SHA-256 `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
The v7 batch profile still samples SHA-256 at 19.75% on the ordered packer.
That required logical hash stays unchanged. The producer trace and source
also show temporary owned-entry allocation and destruction; the prototype
removes those copies. Sampling includes setup/close and cannot by itself
attribute the measured throughput change. V4 flushes its memtable at close,
outside the measured write interval. All profiles are excluded from scoring.

The source base is `3ad46356`. The candidate executable is
`target/release/write-perf-v7-borrowed-preparation`, SHA-256
`1ec5ef47699236c329c1837981cb6f2fcd49cd522311ceb7e4e0352a83e2a31e`.
The [128-input manifest](artifacts/rfc-025-v7-borrowed-preparation-20261026-source-sha256.tsv)
has SHA-256 `6d473975d1ba8faadff0f7e7256211f3d72d82d4dc9df3e988606a03a7fe07e2`.
The [source patch](artifacts/rfc-025-v7-borrowed-preparation-20261026-rejected.patch)
applies to that base with `git apply --unidiff-zero`. Both release builds use:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

Each cell has four warmups and five alternating four-arm pairs: old/new
executables with ordinary v4 Parallel and PITR v7 Parallel. Eight writers use
1-KiB values and a 1-GiB SST target. Point cells write 50,000 puts; batch64
cells write 100,000 puts. A single-writer guard uses the same point workload.
All runs use fresh databases; setup and close are outside the timed interval.
The host is the same Intel Core i9-13900T and Solidigm SSDPFKNU010TZ, with ext4
on `/dev/nvme0n1p3`. Tmpfs uses three seconds between records; ext4 has an
initial 30-second cooldown and ten seconds between records. Builds, tests,
and profiling never overlap scored comparisons.

The schedule starts with tmpfs batch64 and point8. It advances only if the
batch screen reaches baseline in both raw and normalized throughput. If it
passes, a solo tmpfs guard, ext4 point guard, and two fixed ext4 batch64
comparisons follow. Retention requires the pooled ten SSD batch pairs to reach
baseline raw and normalized throughput without increasing CPU; an ext4 point
raw loss above 5% also rejects the change. There are no peak-selected repeats.

Ratios are medians of paired candidate/baseline ratios. Normalization compares
each v7 rate relative to its same-executable v4 control. CPU is process user
plus system time. Batch64 has only 16 sampled latency observations per run;
point8 has 504 and point1 has 500. All stalls and unfavorable controls remain
included.

| Cell | Raw throughput | v4-normalized | CPU | Sampled p99 |
| --- | ---: | ---: | ---: | ---: |
| tmpfs batch64, 8 writers | 1.046x | 1.034x | 0.975x | 1.063x |
| tmpfs point, 8 writers | 1.017x | 1.056x | 0.975x | 0.819x |
| tmpfs point, 1 writer | 1.002x | 1.015x | 1.030x | 0.983x |
| ext4 batch64 screen | 0.954x | 0.931x | 1.044x | 0.847x |
| ext4 batch64 confirmation | 0.983x | 0.866x | 0.957x | 0.986x |
| ext4 point, 8 writers | 1.025x | 1.041x | 0.976x | 0.962x |

All 120 scored records, 24 warmups, and three unscored profiles are preserved.
The solo guard changes raw throughput +0.2% and normalized
throughput +1.5%. The pooled SSD batch sampled p50/p99 ratios
are 1.072x/0.947x. The complete controls and latency data
limit any uniform no-regression claim. The fixed retention guard evaluates to
`false`; no extra repeats follow this outcome. This does not qualify
the full RFC Stage 8 workload or operational gate.

The captured median group count falls from 1,252 to 1,192.5 and sync count from
361 to 357.5. The corresponding FRONTIER count also falls from 361 to 357.5.
These diagnostics do not show extra syncs explaining the loss. The harness
resets them after setup and captures them before drain/close. Counter intervals
overlap and do not isolate causality; the raw records and their reproducible
medians remain in the summary. Whole-process perf samples, discussed above,
include setup/close.

## Highest comparable scored ext4 results

| Build / format | Point puts/s, 8 writers | Batch64 puts/s, 8 writers |
| --- | ---: | ---: |
| Strongest prior v4 Parallel | 20,420 | 482,410 |
| Strongest prior retained v7 | 15,759 | 391,992 |
| Immutable-batch baseline in this pass | 15,567 | 384,113 |
| Borrowed-preparation candidate | 15,543 | 371,375 |

The candidate remains 23.9% below the highest v4 point reference and
23.0% below its batch64 reference. Peaks from separate runs are separate
from the paired analysis and never replace stronger historical measurements.

## Reproduction

The [analyzer](artifacts/rfc-025-v7-borrowed-preparation-20261026-analyze.py)
checks raw and executable hashes, complete pairs, operation counts, settings,
and latency sample counts, and reproduces the
[summary](artifacts/rfc-025-v7-borrowed-preparation-20261026-summary.json).
All 147 run IDs are distinct. Applying the rejected patch to `3ad46356`
reproduces all 128 source hashes. The summary regenerates exactly from the
records; all six drivers pass shell syntax checks and remove their scratch data.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs batch64, 8 writers | [script](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-batch64-8writers-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-batch64-8writers-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-batch64-8writers-20261026.jsonl) |
| tmpfs point, 8 writers | [script](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-8writers-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-8writers-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-8writers-20261026.jsonl) |
| tmpfs point, 1 writer | [script](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-1writer-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-1writer-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-borrowed-preparation-tmpfs-1writer-20261026.jsonl) |
| ext4 batch64 screen | [script](artifacts/rfc-025-v7-borrowed-preparation-ext4-batch64-8writers-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-borrowed-preparation-ext4-batch64-8writers-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-borrowed-preparation-ext4-batch64-8writers-20261026.jsonl) |
| ext4 batch64 confirmation | [script](artifacts/rfc-025-v7-borrowed-preparation-confirm-ext4-batch64-8writers-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-borrowed-preparation-confirm-ext4-batch64-8writers-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-borrowed-preparation-confirm-ext4-batch64-8writers-20261026.jsonl) |
| ext4 point, 8 writers | [script](artifacts/rfc-025-v7-borrowed-preparation-ext4-8writers-20261026-run.sh) | [TSV](artifacts/rfc-025-v7-borrowed-preparation-ext4-8writers-20261026-runs.tsv) | [JSONL](artifacts/rfc-025-v7-borrowed-preparation-ext4-8writers-20261026.jsonl) |

Unscored ext4 CPU profiles: [driver](artifacts/rfc-025-v7-producer-cpu-profile-20261026-run.py),
[commands and hashes](artifacts/rfc-025-v7-producer-cpu-profile-20261026-runs.tsv),
and [raw records](artifacts/rfc-025-v7-producer-cpu-profile-20261026.jsonl).

| Cell | Flat samples | Call paths |
| --- | --- | --- |
| v7 batch64 | [report](artifacts/rfc-025-v7-producer-cpu-profile-20261026-v7-batch64-flat.txt) | [report](artifacts/rfc-025-v7-producer-cpu-profile-20261026-v7-batch64-calls.txt) |
| v7 point | [report](artifacts/rfc-025-v7-producer-cpu-profile-20261026-v7-point-flat.txt) | [report](artifacts/rfc-025-v7-producer-cpu-profile-20261026-v7-point-calls.txt) |
| v4 batch64 | [report](artifacts/rfc-025-v7-producer-cpu-profile-20261026-v4-batch64-flat.txt) | [report](artifacts/rfc-025-v7-producer-cpu-profile-20261026-v4-batch64-calls.txt) |

Profile text only trims display padding; symbols are preserved. Drivers refuse
to overwrite captures and clean their disposable databases. All previous eleven
optimizations remain, and the maintenance-state prototype stays reverted.

[WAL documentation](../README.md).
