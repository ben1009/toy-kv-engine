# RFC 025: cached extent readiness and cooled ext4 comparisons, 2026-10-25

**Result: retain the extent readiness cache; v4 parity remains unmet.**

The cache removes redundant preparation locks and inode queries from the v7
FRONTIER path. Eight-writer point writes improve modestly on tmpfs and ext4;
the low-concurrency and batch64 controls remain mixed. V4 parity is not
established. The preceding [ordered encoding change](rfc-025-v7-packer-encoding-20261025.md)
remains in place. A separate packer-wakeup experiment is rejected.

## Diagnosis and rejected wakeup experiment

A fresh retained-build profile sampled 100,000 single puts with eight writers
on tmpfs using `perf record -e cpu-clock:u -F 997 --call-graph dwarf,8192`.
It captured 1,954 v7 samples and 1,843 v4 samples without lost samples. V7's
ordered-packer receive accounts for 8.75% of sampled user CPU; the sync
coordinator's completion receive accounts for 10.29%. The profile includes
setup and close, including v4's memtable drain, so its percentages identify
candidate work rather than isolate a throughput delta. The binary is the
retained encoding build, SHA-256
`9a9a108ae23a3928ad88e42664ecc79c85a8e2d3f8ed5f81ef6aeb3b034e469e`.

Replacing the packer's bounded boolean wake channel with a coalesced atomic
request bit and thread parking passes all 1,961 tests, including three new
wakeup/shutdown tests. It nevertheless lowers median paired v7 throughput
22.4%, lowers the normalized v7:v4 ratio 20.1%, and raises process CPU 21.3%
in five tmpfs pairs. Sampled p50 rises 36.3%. The candidate binary is
`160fd69137403428444c55f260a370ea95380f9fb0187002e6812d14e7fb9509`.
That code and its three tests are reverted; the bounded wake channel remains.
No ext4 runs are needed to reject this clear CPU regression.

The [profile driver](artifacts/rfc-025-v7-packer-encoding-cpu-profile-20261025-run.py),
[profile manifest](artifacts/rfc-025-v7-packer-encoding-cpu-profile-20261025-runs.tsv),
[profile JSONL](artifacts/rfc-025-v7-packer-encoding-cpu-profile-20261025.jsonl),
and [v7](artifacts/rfc-025-v7-packer-encoding-cpu-profile-20261025-v7.txt) /
[v4](artifacts/rfc-025-v7-packer-encoding-cpu-profile-20261025-v4.txt) reports
retain the diagnostic commands and measurements. These two profiled runs do
not enter scored comparisons. Rejected-wakeup measurements have their own
[driver](artifacts/rfc-025-v7-packer-wake-tmpfs-8writers-20261025-run.sh),
[manifest](artifacts/rfc-025-v7-packer-wake-tmpfs-8writers-20261025-runs.tsv),
[JSONL](artifacts/rfc-025-v7-packer-wake-tmpfs-8writers-20261025.jsonl),
[analyzer](artifacts/rfc-025-v7-packer-wake-20261025-analyze.py), and
[summary](artifacts/rfc-025-v7-packer-wake-20261025-summary.json).

## Extent readiness cache

The retained baseline already has a checked shared preparation cursor for
v7 DATA and FRONTIER writes. However, each FRONTIER still takes the preparation
mutex and queries the inode length even when its entire allocation is ready.
A new atomic runtime cursor publishes the end of successful preparation.
Requests inside that prefix return without either operation. Growth still
holds the preparation mutex, rechecks the published cursor, and performs the
same allocation/initialization before advancing the cache.

This cache describes space ready to accept writes. It does not describe
completed DATA, a durable ticket, or a recovery authority. A large DATA group
can prepare space by allocation alone under the existing policy; later small
writes must still avoid zeroing that space. Partial initialization does not
publish its whole requested suffix. The existing extent-failure test now
checks that property explicitly, while successful growth, byte preservation,
partial-frame caps, recovery, and lifecycle tests continue to pass.

The cache is private to each runtime and starts conservatively at its validated
installed append offset. Recovered/replaced inodes construct a new runtime.
No frame encoding, checksum, digest, acknowledgment ordering, one-sync rule,
resource limit, or format default changes. Runtime-owned WALs are not truncated
while admission is active.

Only `kv-engine/src/wal/parallel/runtime.rs` changes in this pass. The candidate
binary SHA-256 is
`61d59ba0e739ec67338979a4a4a259e06d1940c1e92c49ff72cf38bdd32fb75e`.
Both executables use
`cargo build --release --package kv-engine --bin write-perf --features bench`
and the pinned nightly toolchain. The source is
`f08f4cd7f0bac424d9732837401a89a76ec5b5b6` plus local optimization changes.
The [source manifest](artifacts/rfc-025-v7-cached-extents-20261025-source-sha256.tsv)
records Rust, Cargo, lockfile, and toolchain hashes; its SHA-256 is
`df54f391afa9aaf87b9ed9a5d798707f87c20aa5802858d0602d0756bc4e0fd3`.

## Measurement protocol

Each cell has both executables' v4 controls and v7/PITR paths, one unscored
warmup per arm, and five alternating four-arm scored pairs. All runs use
Parallel I/O, fresh databases, 1-KiB values, a 1-GiB SST target, and latency
sampling every 100 operations. Single-put runs issue 50,000 puts; batch64
runs issue 100,000 puts in 1,568 batches across eight writers. This batch64
sampler retains only 16 latency observations per run, so its p99 is a sparse
sample and cannot qualify tails. Single-put runs retain 500 observations at one writer and 504 at eight writers.

The host is an Intel Core i9-13900T with 32 logical CPUs, Linux
6.12.71-1-lts, tmpfs on `/dev/shm`, and ext4 on the Solidigm SSDPFKNU010TZ.
Tmpfs drivers idle three seconds between runs. Each ext4 experiment idles
30 seconds before warmups and ten seconds after every run; manifests record
the NVMe temperature after each run. This differs from the preceding report's
three-second idle. Both arms receive the same cooldown. Historical maxima
remain diagnostic references under matching workload settings, rather than
an isolation of cooling or software effects.

Source and executables stay in the local repository; tmpfs holds disposable
benchmark databases only. io_uring tests and benchmarks run outside the
sandbox, sequentially with builds and other CPU benchmarks. Every scored
run stays in the analysis. Setup and close are outside the measured write
interval. Process CPU is measured user plus system CPU time. Raw ratios
compare candidate v7 to baseline v7 in each pair; normalization divides that
ratio by candidate-v4/baseline-v4 throughput from the same pair.

## Paired results

| Workload | Baseline / candidate v7 median puts/s | Median paired raw ratio | Median v4-normalized ratio | Median CPU ratio |
| --- | ---: | ---: | ---: | ---: |
| tmpfs, 8 writers, single puts | 111,033 / 116,504 | 1.035x | 1.054x | 0.967x |
| ext4, 8 writers, single puts | 15,060 / 15,166 | 1.015x | 1.024x | 0.992x |
| tmpfs, 8 writers, batch64 | 613,809 / 647,324 | 1.033x | 0.972x | 0.955x |
| tmpfs, 1 writer, single puts | 43,174 / 42,288 | 0.982x | 0.980x | 0.974x |
| ext4, 8 writers, batch64 | 325,461 / 325,801 | 1.080x | 1.073x | 0.828x |

| Workload | Median paired sampled p50 ratio | Median paired sampled p99 ratio |
| --- | ---: | ---: |
| tmpfs, 8 writers, single puts | 1.016x | 1.037x |
| ext4, 8 writers, single puts | 0.996x | 0.898x |
| tmpfs, 8 writers, batch64 | 1.225x | 1.084x |
| tmpfs, 1 writer, single puts | 1.017x | 0.953x |
| ext4, 8 writers, batch64 | 0.877x | 0.941x |

The eight-writer point-write gain is modest: 3.5% raw / 5.4% normalized on
tmpfs, and 1.5% / 2.4% on ext4. All five ext4 pairs favor candidate v7 in raw
throughput. Sampled p99 falls 10.2% on ext4 but rises 3.7% on tmpfs.

The guards remain mixed. Tmpfs batch64 improves raw throughput 3.3% and
reduces CPU 4.5%, while its normalized ratio falls 2.8% and sampled p50/p99
rise. Its 16 latency observations per run are too sparse for a tail guarantee.
Single-writer tmpfs raw throughput falls 1.8%, normalization falls 2.0%, CPU
falls 2.6%, and sampled p99 falls 4.7%. No broad no-regression claim follows
from these results.

Ext4 batch64's median paired raw throughput improves 8.0%, normalization
improves 7.3%, and CPU falls 17.2%. Arm medians are nearly equal because the
first baseline run stalls. The first scored v4 controls achieve 70,841 /
72,255 puts/s and baseline/candidate v7 achieve 51,956 / 281,577. The next
pair has baseline/candidate v7 at 373,687 / 323,059, favoring the baseline.
All five pairs remain included. Cooldown has not eliminated device variability.

Recorded NVMe temperatures range from 41.85–47.85°C in the point experiment
and 40.85–44.85°C in batch64, including warmups. These sensor readings and
idle periods do not identify the cause of a stall or establish absence of
thermal/device effects.

## Highest comparable ext4 scores

The references use eight writers, 1-KiB values, and the same operation counts
as the current cell. Warmups and rejected candidate binaries do not enter
retained-v7 maxima. All historical references remain available through the
[previous report](rfc-025-v7-packer-encoding-20261025.md) and the
[extent-initialization comparison](rfc-025-v7-parallel-vs-v4-parallel-writer-sweep-20261025.md#shared-v7-extent-initialization-on-ext4).
Peaks are separate runs; their comparison is diagnostic and does not replace
the paired evidence.

| Workload | Highest retained baseline v7 | Cache candidate v7 | Highest comparable v4 | Remaining gap to v4 |
| --- | ---: | ---: | ---: | ---: |
| Single puts, 50,000 operations | 15,278 | 15,759 | 20,420 | 22.8% |
| Batch64, 100,000 operations | 373,687 | 391,992 | 482,410 | 18.7% |

The point-write peak improves 3.1% over the previous retained high. The cache
batch64 peak improves 4.9% over the strongest retained-baseline run in the
same cooled experiment. That baseline itself reaches 373,687 puts/s, above
its preceding report's 341,049 high. Comparing the new 391,992 score only to
341,049 would mix software and environment/run variation. The historical v4
high of 482,410 still exceeds candidate v7, even though this experiment's
highest v4 control is only 373,449.

## Validation and outcome

`cargo make check` passes all 1,958 tests plus formatting, dependency ordering,
all-target all-feature Clippy with warnings denied, unused-dependency checks,
and spelling. Its nextest run ID is
`52579b58-6ea4-4a9d-9d02-50fc2bd7277d`. Failure injection still preserves the
acknowledged prefix and now verifies that partial initialization leaves the
cache at its preceding ready boundary. Successful preparation and byte
preservation tests cover actual WAL growth and recovery.

Retain the small cache change for its concurrent point-write and SSD batch64
gains. The mixed single-writer and tmpfs batch controls remain recorded.
The wakeup replacement stays reverted. This slice does not establish v4
parity, qualify other devices/writer counts, or pass RFC 025's Stage 8 gate.

## Reproduction

The [analyzer](artifacts/rfc-025-v7-cached-extents-20261025-analyze.py) verifies
raw hashes, binary hashes, workload parameters, and latency sample counts.
The [summary JSON](artifacts/rfc-025-v7-cached-extents-20261025-summary.json)
records all paired ratios, winning run IDs and pipeline counters, temperatures,
and historical peak provenance. It uses the retained
[shared analysis helper](artifacts/rfc-025-v7-mvcc-preparation-20261025-analyze.py).
The five cache cells contain 100 scored records and 20 unscored warmups.

| Experiment | Driver | Manifest | Raw records |
| --- | --- | --- | --- |
| tmpfs, 8 writers, single puts | [script](artifacts/rfc-025-v7-cached-extents-tmpfs-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-cached-extents-tmpfs-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-cached-extents-tmpfs-8writers-20261025.jsonl) |
| ext4, 8 writers, single puts | [script](artifacts/rfc-025-v7-cached-extents-ext4-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-cached-extents-ext4-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-cached-extents-ext4-8writers-20261025.jsonl) |
| tmpfs, 8 writers, batch64 | [script](artifacts/rfc-025-v7-cached-extents-tmpfs-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-cached-extents-tmpfs-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-cached-extents-tmpfs-batch64-8writers-20261025.jsonl) |
| tmpfs, 1 writer, single puts | [script](artifacts/rfc-025-v7-cached-extents-tmpfs-1writer-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-cached-extents-tmpfs-1writer-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-cached-extents-tmpfs-1writer-20261025.jsonl) |
| ext4, 8 writers, batch64 | [script](artifacts/rfc-025-v7-cached-extents-ext4-batch64-8writers-20261025-run.sh) | [TSV](artifacts/rfc-025-v7-cached-extents-ext4-batch64-8writers-20261025-runs.tsv) | [JSONL](artifacts/rfc-025-v7-cached-extents-ext4-batch64-8writers-20261025.jsonl) |

[WAL documentation](../README.md).
