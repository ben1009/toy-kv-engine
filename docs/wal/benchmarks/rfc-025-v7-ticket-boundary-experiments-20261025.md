# RFC 025: rejected WAL ticket-boundary cache, 2026-10-25

**Result: reject both ticket-cache variants.** The final variant lowers median
paired ext4 batch64 throughput 6.1% and raises process CPU 13.5%. Its modest
raw tmpfs gain does not replicate the initial relative gain against v4. All
production changes are reverted to the retained
[extent readiness cache](rfc-025-v7-extent-readiness-cache-20261025.md).
The eight accepted optimizations are committed separately; v4 parity remains
unmet.

## Accepted commit split

The previously retained source was split into these independent commits on
`perf/rfc025-v4-parallel-first-leg`. The first six intermediate trees pass
`cargo make check`; the last two match the source hashes from their successful
full checks. The split preserved the final source and all 201 previously
changed files byte-for-byte. This follow-up updates the measurement documents;
no rejected implementation is included in the production commits.

| Commit | Retained optimization |
| --- | --- |
| `d976783d` | Hash maps for v7 reservation accounting |
| `b42045f6` | Separate bounded pool for v7 single-frame DATA buffers |
| `a26f136b` | Bounded v7 coalescing after fast syncs |
| `ae54a9d6` | Defer physical hashing and logical digest finalization |
| `3f89a69a` | Owned v7 preparation before MVCC sequencing |
| `e2b9e8a2` | Shared DATA/FRONTIER extent preparation |
| `f362d148` | DATA framing and logical hashing in the ordered packer |
| `ac66f110` | Cache successfully prepared extent prefixes |

`3e2d1c62` records the preceding benchmark reports, raw records, and provenance.
The rejected experiment below starts from that commit.

## Experiment and validation

Synchronous commit waiters previously validated their ticket by taking the admission mutex
while holding the durability mutex. The candidate publishes the assigned
exclusive ticket end into a runtime-local atomic after enqueueing a batch,
before releasing admission. Ticket validation and batch-count queries can
then avoid the admission lock. The counter never represents durability: a
waiter still requires the synced durable frontier or a poison result.

The initial implementation rejects tickets outside the cached end directly.
The final implementation treats the cache as a lower bound and rechecks
authoritative admission before rejecting a miss. A new real-v7 regression
forces an older cached end after an acknowledged ticket, then checks that
sync and async waits accept that ticket and reject the next unassigned one.
The capacity-rejection test also checks that a rejected prepared batch does
not make another ticket valid. Recovered counters start at the accepted
recovery ticket end.

The initial build passes all 1,958 tests. The final build passes all 1,959
under `cargo make check`, including formatting, dependency ordering, all-target
all-feature Clippy with warnings denied, unused-dependency checks, and spelling.
The final run includes one retry of the WAL-disabled
`simple_leveled_compaction::test_integration`: its first attempt times out at
30 seconds, and the retry passes in 3.544 seconds. The trial introduces no
compaction change. The final nextest run ID is `ea6e95e8-8f7f-48fc-9d14-2c69fac563a4`.

Both implementations are removed after measurement. The restored production
source matches the committed extent-cache source manifest and its successful
1,958-test full check. Neither the ticket counter nor its tests remain in the
production tree.

## Executables and protocol

| Build | Binary SHA-256 |
| --- | --- |
| Retained extent cache | `61d59ba0e739ec67338979a4a4a259e06d1940c1e92c49ff72cf38bdd32fb75e` |
| Initial ticket cache | `f73de270f4949eb9a23e117e68a2837e450b3d7ebc1f831d560a437717116d54` |
| Final ticket cache with authoritative rejection | `c44c33d611466e9fe3274fd6f36d7ec5668454780f5460f34ac52137b144fdd1` |

Each build uses
`cargo build --release --package kv-engine --bin write-perf --features bench`
with the pinned nightly toolchain. Sources and executables stay in the local
repository. Tmpfs holds disposable benchmark databases; each run removes its
own data. io_uring tests, builds, and benchmarks run sequentially outside the
sandbox.

Every cell includes both binaries' v4 controls and v7/PITR paths, one warmup
per arm, and five alternating four-arm scored pairs. All runs use fresh
databases, Parallel I/O, 1-KiB values, and a 1-GiB SST target. Single-put runs
issue 50,000 puts; batch64 runs issue 100,000 puts. Latency sampling every
100 operations retains 504 samples at eight point writers, 500 at one writer,
and only 16 batch64 observations. Batch64 p99 remains a sparse sample.

The host is an Intel Core i9-13900T with 32 logical CPUs, Linux
6.12.71-1-lts, tmpfs on `/dev/shm`, and ext4 on the Solidigm SSDPFKNU010TZ.
Tmpfs idles three seconds between runs. Ext4 idles 30 seconds before each
experiment and ten seconds after each run; manifests retain NVMe sensor
readings. Every scored run stays included. Process CPU sums measured user
and system time. The normalized ratio divides candidate-v7/baseline-v7
throughput by the candidate-v4/baseline-v4 ratio from the same pair.

## Complete paired results

Initial and final builds are analyzed separately. The final version repeats
the CPU point-write screen and the unresolved SSD batch64 guard; the other
three initial cells are not measurements of the final binary.

| Variant / workload | Baseline / candidate v7 median puts/s | Median paired raw ratio | Median v4-normalized ratio | Median CPU ratio |
| --- | ---: | ---: | ---: | ---: |
| Initial: tmpfs, 8 writers, single puts | 113,172 / 119,120 | 1.068x | 1.121x | 0.931x |
| Initial: ext4, 8 writers, single puts | 15,422 / 6,220 | 1.008x | 1.003x | 0.961x |
| Initial: tmpfs, 8 writers, batch64 | 613,955 / 593,554 | 1.043x | 1.032x | 0.984x |
| Initial: tmpfs, 1 writer, single puts | 41,885 / 44,538 | 1.065x | 1.136x | 0.928x |
| Initial: ext4, 8 writers, batch64 | 340,049 / 121,923 | 0.983x | 0.922x | 1.020x |
| Final: tmpfs, 8 writers, single puts | 116,504 / 120,335 | 1.034x | 0.995x | 0.975x |
| Final: ext4, 8 writers, batch64 | 358,965 / 339,829 | 0.939x | 0.840x | 1.135x |

| Variant / workload | Median paired sampled p50 ratio | Median paired sampled p99 ratio |
| --- | ---: | ---: |
| Initial: tmpfs, 8 writers, single puts | 0.954x | 0.821x |
| Initial: ext4, 8 writers, single puts | 1.011x | 1.048x |
| Initial: tmpfs, 8 writers, batch64 | 1.078x | 1.115x |
| Initial: tmpfs, 1 writer, single puts | 0.951x | 0.916x |
| Initial: ext4, 8 writers, batch64 | 0.983x | 1.093x |
| Final: tmpfs, 8 writers, single puts | 0.986x | 1.289x |
| Final: ext4, 8 writers, batch64 | 0.810x | 0.965x |

The initial tmpfs screen suggests a 12.1% normalized eight-writer gain and
13.6% at one writer. The final repeat has only 3.4% higher paired raw
throughput, a 0.5% lower normalized ratio, and 28.9% higher sampled p99.
The initial relative improvement does not replicate in the final build.

Initial ext4 point writes have substantial shared stalls: the first old/new
v4 controls reach 5,111 / 4,973 puts/s and v7 reaches 4,565 / 4,615. The next
pair has baseline/candidate v7 at 15,626 / 6,220. The five-pair raw median
ratio is near one despite the very different arm medians. Its candidate peak
is 15,581 puts/s, below the retained 15,759 high; no SSD point-write gain is
established.

The initial SSD batch64 comparison has 1.7% lower paired raw throughput and a
7.8% lower normalized ratio, prompting the final repeat. In that repeat,
all five v7 raw comparisons favor the baseline. The median raw loss is 6.1%,
the normalized loss is 16.0%, and CPU rises 13.5%. The final candidate peaks
at 343,716 puts/s versus its same-experiment baseline's 364,800; both are
below the retained v7 high of 391,992. This is sufficient to reject the
additional state and synchronization without inferring a hardware or compiler
cause from the measurements.

## Retained highest scores

Only retained builds enter the v7 high. Rejected candidates and warmups are
excluded. Counts, writer concurrency, and value size match each reference;
peaks are separate runs and remain diagnostic rather than evidence of parity.

| Ext4, 8 writers | Highest retained v7 puts/s | Highest comparable v4 puts/s | Remaining gap |
| --- | ---: | ---: | ---: |
| Single puts, 50,000 operations | 15,759 | 20,420 | 22.8% |
| Batch64, 100,000 operations | 391,992 | 482,410 | 18.7% |

The retained cache source and default release executable are restored. This
experiment does not establish a new retained performance high or meet the
RFC 025 Stage 8 gate.

## Reproduction and evidence

The [initial analyzer](artifacts/rfc-025-v7-ticket-snapshot-20261025-analyze.py)
and [summary](artifacts/rfc-025-v7-ticket-snapshot-20261025-summary.json), plus
the [final analyzer](artifacts/rfc-025-v7-ticket-lookup-20261025-analyze.py)
and [summary](artifacts/rfc-025-v7-ticket-lookup-20261025-summary.json), validate
record hashes, binary hashes, parameters, sample counts, and peak provenance.
The source manifests for the
[initial](artifacts/rfc-025-v7-ticket-snapshot-20261025-source-sha256.tsv) and
[final](artifacts/rfc-025-v7-ticket-lookup-20261025-source-sha256.tsv) candidates
bind their exact rejected Rust sources. Their SHA-256 values are respectively
`85633344a95a90932f2ee2c222694f4d363b3e1eeb0501d941812dcc1bb0e070`
and `c548b6d394d753cf153900113690252c4456fdf4da55ab12afef1b80c476eb38`.
The exact [initial](artifacts/rfc-025-v7-ticket-snapshot-20261025-initial.patch)
and [final](artifacts/rfc-025-v7-ticket-lookup-20261025-final.patch) patches apply
to `3e2d1c62` with `git apply --unidiff-zero`. They are experiment artifacts,
not production changes. The seven cells retain 140 scored records and 28
warmups, including every unfavorable control.

| Variant / experiment | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| Initial: tmpfs, 8 writers, single puts | [script](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-8writers-20261025.jsonl) |
| Initial: ext4, 8 writers, single puts | [script](artifacts/rfc-025-v7-ticket-snapshot-ext4-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-ticket-snapshot-ext4-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ticket-snapshot-ext4-8writers-20261025.jsonl) |
| Initial: tmpfs, 8 writers, batch64 | [script](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-batch64-8writers-20261025.jsonl) |
| Initial: tmpfs, 1 writer, single puts | [script](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-1writer-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-1writer-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ticket-snapshot-tmpfs-1writer-20261025.jsonl) |
| Initial: ext4, 8 writers, batch64 | [script](artifacts/rfc-025-v7-ticket-snapshot-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-ticket-snapshot-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ticket-snapshot-ext4-batch64-8writers-20261025.jsonl) |
| Final: tmpfs, 8 writers, single puts | [script](artifacts/rfc-025-v7-ticket-lookup-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-ticket-lookup-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ticket-lookup-tmpfs-8writers-20261025.jsonl) |
| Final: ext4, 8 writers, batch64 | [script](artifacts/rfc-025-v7-ticket-lookup-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-ticket-lookup-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-ticket-lookup-ext4-batch64-8writers-20261025.jsonl) |

[WAL documentation](../README.md).
