# RFC 025: immutable logical batches in the v7 packer, 2026-10-25

**Result: retain immutable encoder-owned logical batches; v4 parity remains unmet.**

Across ten ext4 batch64 pairs, median raw throughput improves 10.1%,
v4-normalized throughput improves 5.6%, and process CPU falls 8.0%. Point and
control comparisons remain mixed. The candidate does not replace the stronger
historical peak references.

## Change and correctness

The retained source change is committed independently as `c5b344a5`.

The producer already encodes canonical entries and computes their entry CRC.
Admission fills the ordered ticket, commit timestamp, and recorded time, then
computes the logical-header CRC. Previously, the ordered packer decoded that
fresh logical header and reread the entire entry stream to check its CRC
before framing it. The change removes that repeated pass for codec-owned
logical batches.

`EncodedWalV7LogicalBatch` has private byte storage and a private ticket. Its
only constructor consumes the validated entry encoder and checks the final
ordered metadata. Callers can read its bytes but cannot mutate them or create
it from arbitrary input. Its DATA materializer still checks the assigned
ticket, output capacity, alignment, and complete physical range before writing.
The raw-byte materializer retains its full header/data CRC checks and shares
only the frame-writing loop with the owned path. Persisted frame/header CRCs,
logical SHA-256 preimages, padding, sync order, and recovery remain unchanged.
The owned path removes a redundant write-side validation; it does not remove
checksum generation or validation of bytes read from storage.

Runtime preparation and admission no longer carry a second copy of
`WalV5Limits`; the entry encoder applies those limits while constructing the
owned bytes. The change adds no allocation, buffer, resource charge, public
configuration, or durability authority.

The existing post-admission failure test previously mutated a queued `Vec` to
create a CRC mismatch. Immutable queued bytes no longer expose that operation.
The fixture now substitutes a valid codec-produced batch with a mismatched
ticket. The test still checks group poisoning, rejection of later writes,
retention of the acknowledged prefix, cleanup of buffer charges, and recovery.
Raw corrupt-CRC rejection remains covered by the checked codec tests.

Two new tests compare 20 single-point and mixed duplicate/range batches across
fragment boundaries and the maximum valid value size. They compare logical
bytes and every complete output frame with the checked encoder, including
CRCs, padding, unchanged output tails, and independent structural decoding.
Invalid final metadata, tickets, offsets, and capacities must reject before
changing output. The existing golden vectors and crash/recovery suite run too.

`cargo make check` passes outside the io_uring sandbox: 1,963 tests pass, zero
skips/retries. One unchanged leveled-compaction test takes 32.4 seconds. Nextest
run: `0a2c1f1f-7b6d-4928-8c78-3259b03ae741`. An initial lint correction uses
fixed-size `as_chunks`; an initial new fixture exceeded the 65,535-byte value
limit and was corrected to valid boundary sizes. Both failed-check logs remain
locally alongside the final successful `check.log` under
`target/wal-opt/20261025-v7-validated-logical-batch`. Release build passes.

## Profiling and measurement protocol

Fresh CPU profiles use the retained coalescing executable, SHA-256
`b7418dda91701807a76809a094eaa0538540efcac9a1cb0f683881f9ad02ae34`, from
source `34a8b334` (documentation baseline `49f3f9c5`). The batch64 profile
samples SHA-256 at 17.12% and specialized CRC at 2.47% on the ordered packer.
They identify work to examine, not the throughput gain attributable to a
particular instruction. Profiles include process setup and close; v4 also
flushes its memtable on close, outside the timed write interval. Every profile
is excluded from scoring.

The candidate executable is `target/release/write-perf-v7-validated-logical-batch`,
SHA-256 `614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9`.
Its [source manifest](artifacts/rfc-025-v7-validated-logical-batch-20261025-source-sha256.tsv)
covers 128 Rust/build inputs and has SHA-256
`4e359052d5e6c6aa29fe5009054112f2fcbf463d0a2c5836f428c2491c6c0630`.
The complete [source patch](artifacts/rfc-025-v7-validated-logical-batch-20261025-candidate.patch)
applies to `49f3f9c5` with `git apply --unidiff-zero`. Both executables use:

```bash
cargo build --release --package kv-engine --bin write-perf --features bench
```

The host remains an Intel Core i9-13900T, Linux 6.12.71-1-lts, and ext4 on the
Solidigm SSDPFKNU010TZ, with tmpfs as a CPU-side comparison. Each cell uses four
unscored warmups and five alternating four-arm pairs: baseline/candidate
binaries with ordinary v4 Parallel and PITR v7 Parallel. Fresh databases use
1-KiB values and a 1-GiB SST target. Single-put runs issue 50,000 puts; batch64
runs issue 100,000 puts. Setup and close are outside the timed write interval.
Tmpfs records idle three seconds apart. SSD cells cool for 30 seconds initially
and ten seconds between records. Builds, checks, and profiling finish before
scoring begins. All runs and moving v4 controls remain included.

A fixed five-pair SSD batch64 confirmation follows a promising screen, with
point-write and solo guards before retention. Additional repeats are not
selected by a peak. Throughput/CPU figures below are medians of paired ratios;
normalized throughput divides each v7 score by its same-binary v4 control
before comparing binaries. CPU is measured process user plus system time.
Batch64 latency has only 16 samples per record; point writes have 504 with
eight writers and 500 with one writer.

| Cell | Raw throughput | v4-normalized throughput | CPU | Sampled p99 |
| --- | ---: | ---: | ---: | ---: |
| tmpfs, batch64, 8 writers | 1.177x | 1.106x | 0.900x | 0.899x |
| tmpfs, single puts, 8 writers | 0.949x | 1.042x | 1.030x | 0.717x |
| ext4, batch64, screen | 1.073x | 1.086x | 0.844x | 1.000x |
| ext4, batch64, confirmation | 1.128x | 0.987x | 0.975x | 1.010x |
| tmpfs, single puts, 1 writer | 1.045x | 1.022x | 0.985x | 0.949x |
| ext4, single puts, 8 writers | 1.011x | 0.998x | 0.983x | 0.971x |

The SSD confirmation reproduces the raw batch throughput and CPU direction,
but its normalized ratio is `0.987x`; the full ten-pair normalized result is
`1.056x`. The confirmation's best baseline/candidate v7
scores are only 169,775 / 275,901 puts/s, below both screen peaks. These storage
stalls and moving v4 controls remain included. Sampled batch p50 rises
10.7% across ten pairs, while p99 is almost unchanged;
16 samples per record limit latency conclusions.

Tmpfs batch64's paired raw gain is 17.7%, with 10.0% less CPU and a 10.6%
normalized gain. Tmpfs point8 loses 5.1% raw throughput with 3.0% more CPU,
although its normalized ratio improves 4.2%. The ext4 point-write guard changes
raw throughput +1.1%, normalized throughput
-0.2%, and CPU -1.7%.
Solo tmpfs changes raw throughput +4.5%, normalized
throughput +2.2%, and CPU -1.5%.
These results do not establish uniform absence of regression. The change is
retained for its repeated batch gain and lower batch CPU, with the point and
control limits preserved.

There are 120 scored records and 24 warmups across six fixed cells, plus three
unscored diagnostic profiles. No further SSD repeats are selected by a peak.

## Highest comparable scored results

| Build / format | Single puts, 8 writers | Batch64, 8 writers |
| --- | ---: | ---: |
| Highest comparable prior v4 Parallel | 20,420 | 482,410 |
| Highest prior retained v7 extent cache | 15,759 | 391,992 |
| Retained coalescing baseline in this pass | 15,759 | 341,322 |
| Immutable logical-batch candidate | 15,541 | 381,813 |

The candidate is 23.9% below the highest v4 point reference and
20.9% below its batch64 reference. Historical peaks remain comparisons across
different runs; paired results retain the contemporaneous controls. Neither
v4 parity nor the Stage 8 performance gate is established.

The strongest current v4 controls, every winning run ID, and temperatures
remain in the summary alongside these earlier references.

## Reproduction artifacts

The [analyzer](artifacts/rfc-025-v7-validated-logical-batch-20261025-analyze.py)
validates raw-record/binary hashes, workload settings, counts, and complete
pairs, and reproduces the [summary](artifacts/rfc-025-v7-validated-logical-batch-20261025-summary.json).
It preserves best run IDs and pipeline counters alongside paired comparisons.

| Cell | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs, batch64, 8 writers | [script](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-batch64-8writers-20261025.jsonl) |
| tmpfs, single puts, 8 writers | [script](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-8writers-20261025.jsonl) |
| ext4, batch64, screen | [script](artifacts/rfc-025-v7-validated-logical-batch-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-validated-logical-batch-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-validated-logical-batch-ext4-batch64-8writers-20261025.jsonl) |
| ext4, batch64, confirmation | [script](artifacts/rfc-025-v7-validated-logical-batch-confirm-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-validated-logical-batch-confirm-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-validated-logical-batch-confirm-ext4-batch64-8writers-20261025.jsonl) |
| tmpfs, single puts, 1 writer | [script](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-1writer-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-1writer-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-validated-logical-batch-tmpfs-1writer-20261025.jsonl) |
| ext4, single puts, 8 writers | [script](artifacts/rfc-025-v7-validated-logical-batch-ext4-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-validated-logical-batch-ext4-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-validated-logical-batch-ext4-8writers-20261025.jsonl) |

Diagnostic profiles: [driver](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025-run.py),
[commands/hashes](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025-runs.tsv),
and [raw records](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025.jsonl).

| Diagnostic cell | Flat samples | Call paths |
| --- | --- | --- |
| v7 batch64 | [report](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025-v7-batch64-flat.txt) | [report](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025-v7-batch64-calls.txt) |
| v7 single puts | [report](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025-v7-point-flat.txt) | [report](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025-v7-point-calls.txt) |
| v4 single puts | [report](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025-v4-point-flat.txt) | [report](artifacts/rfc-025-v7-coalescing-cpu-profile-20261025-v4-point-calls.txt) |

The default `target/release/write-perf` executable has the same candidate hash,
and the committed source matches all 128 recorded build inputs.

Profile text strips display padding only; original captures remain in
`target/wal-opt/20261025-v7-remaining-cost`. Disposable databases are removed
after each run. All ten earlier retained optimizations remain in place.

[WAL documentation](../README.md).
